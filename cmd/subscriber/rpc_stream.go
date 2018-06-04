package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/hashicorp/go-multierror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/uuid"
	"github.com/hpidcock/go-pub-sub-channel"

	"github.com/hpidcock/pub2sub/pkg/deadline_list"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

var (
	ErrInvalidMessage = errors.New("invalid message")
	ErrBadChannelID   = status.Error(codes.InvalidArgument, "bad channel id")
	ErrChannelClosing = errors.New("channel closing")
)

func (p *Provider) Stream(req *pb.StreamRequest,
	call pb.SubscribeService_StreamServer) (outErr error) {
	defer func() {
		log.Printf("stream %s closed with %v\n", req.ChannelId, outErr)
	}()

	log.Printf("new stream %s\n", req.ChannelId)
	channelID, err := uuid.Parse(req.ChannelId)
	if err != nil {
		return ErrBadChannelID
	}

	ctx, cancelFunc := context.WithCancel(call.Context())
	defer cancelFunc()

	log.Printf("acquiring %s\n", req.ChannelId)
	err = p.channelClient.AcquireChannel(ctx, channelID, func(ctx context.Context,
		serverID uuid.UUID, channelID uuid.UUID) error {
		return p.evict(ctx, serverID, channelID)
	})
	if err != nil {
		return err
	}
	var onReleasedChan chan<- error
	defer func() {
		// Sadly must be a new context incase the parent is canceled.
		// TODO: Pass context metadata for trace spans.
		releaseCtx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancelFunc()
		err := p.channelClient.ReleaseChannel(releaseCtx, channelID)
		if onReleasedChan != nil {
			onReleasedChan <- err
		}
		if err != nil {
			outErr = multierror.Append(outErr, err)
		}
	}()

	reliable := req.Reliable
	resumed := false
	if reliable {
		log.Printf("stream %s obtaining reliable queue\n", req.ChannelId)
		resumed, err = p.topicController.CreateOrExtendQueue(ctx, channelID,
			p.config.QueueKeepAliveDuration)
		if err != nil {
			return err
		}
	} else {
		log.Printf("stream %s deleting reliable queue\n", req.ChannelId)
		err = p.topicController.DeleteQueue(ctx, channelID)
		if err != nil {
			return err
		}
	}

	// Subscribe after acquire so we don't evict ourselves.
	log.Printf("stream %s subscribing to cross routine messages\n", req.ChannelId)
	internalChannel := p.router.Subscribe(channelID.String())

	var acks *deadline_list.List
	if reliable {
		// TODO: Configurable deadline
		acks = deadline_list.New(2 * time.Second)
	}
	defer func() {
		done := p.router.Unsubscribe(channelID.String(), internalChannel)
		if reliable {
			oldAcks := acks.Close()
			for _, ack := range oldAcks {
				switch msg := ack.(type) {
				case *router.Message:
					msg.Result <- ErrChannelClosing
				}
			}
		}
		for {
			select {
			case <-done:
				return
			case msg, ok := <-internalChannel:
				if ok == false {
					return
				}
				msg.Result <- ErrChannelClosing
			}
		}
	}()

	// Send open event so client knows it can subscribe to stuff.
	err = call.Send(&pb.StreamResponse{
		Event: &pb.StreamResponse_StreamOpenedEvent{
			StreamOpenedEvent: &pb.StreamOpenedEvent{
				Resumed: resumed,
			},
		},
	})
	if err != nil {
		return err
	}

	handleMessage := func(msg router.Message) error {
		switch obj := msg.Obj.(type) {
		case *pb.InternalEvictRequest:
			// This will be posted to in the defered close method.
			onReleasedChan = msg.Result
			return context.Canceled
		case *pb.InternalAckRequest:
			if acks == nil {
				return nil
			}

			ack := acks.Remove(obj.AckId)
			if ack == nil {
				// Worst case scenario, the message is redelivered.
				msg.Result <- nil
				return nil
			}

			switch ackMsg := ack.(type) {
			case *router.Message:
				ackMsg.Result <- nil
			case string:
				// stream id
				// TODO: XDEL in redis
			}

			msg.Result <- nil
			return nil
		case *pb.InternalPublishRequest:
			if reliable && obj.Reliable {
				ackID := acks.Push(msg)
				err = call.Send(&pb.StreamResponse{
					Event: &pb.StreamResponse_StreamMessageEvent{
						StreamMessageEvent: &pb.StreamMessageEvent{
							Id:       obj.Id,
							Ts:       obj.Ts,
							Message:  obj.Message,
							Reliable: true,
							TopicId:  obj.TopicId,
							AckId:    ackID,
						},
					},
				})
				if err != nil {
					acks.Remove(ackID)
					msg.Result <- err
					return err
				}
			} else {
				err = call.Send(&pb.StreamResponse{
					Event: &pb.StreamResponse_StreamMessageEvent{
						StreamMessageEvent: &pb.StreamMessageEvent{
							Id:       obj.Id,
							Ts:       obj.Ts,
							Message:  obj.Message,
							Reliable: false,
							TopicId:  obj.TopicId,
						},
					},
				})
				if err != nil {
					msg.Result <- err
					return err
				}
				msg.Result <- nil
			}
			return nil
		default:
			return ErrInvalidMessage
		}
	}

	var refreshChannel <-chan time.Time
	if reliable {
		refreshChannelDuration := p.config.MaxSubscribeDuration / 2
		ticker := time.NewTicker(refreshChannelDuration)
		defer ticker.Stop()
		refreshChannel = ticker.C
	}

	log.Printf("stream %s waiting for events\n", req.ChannelId)
	for {
		select {
		case <-ctx.Done():
			log.Printf("stream %s context canceled\n", req.ChannelId)
			return context.Canceled
		case msg := <-internalChannel:
			log.Printf("stream %s got cross event %v\n", req.ChannelId, msg.Obj)
			err = handleMessage(msg)
			if err != nil {
				return err
			}
		case <-acks.Wait():
			ack := acks.Pop()
			if ack == nil {
				continue
			}
			switch msg := ack.(type) {
			case *router.Message:
				msg.Result <- context.DeadlineExceeded
			case string:
				// stream id
				// TODO: handle reset stream read id for redelivery
			}
		case <-refreshChannel:
			log.Printf("stream %s extending queue lease\n", req.ChannelId)
			err = p.topicController.ExtendQueue(ctx,
				channelID, p.config.QueueKeepAliveDuration)
			if err != nil {
				return err
			}
		}
	}
}
