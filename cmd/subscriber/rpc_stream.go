package main

import (
	"context"
	"errors"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/hpidcock/go-pub-sub-channel"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

var (
	ErrInvalidMessage = errors.New("invalid message")
)

func (p *Provider) Stream(req *pb.StreamRequest,
	call pb.SubscribeService_StreamServer) error {
	var err error
	channelID := req.ChannelId
	_, err = uuid.Parse(channelID)
	if err != nil {
		return status.Error(codes.InvalidArgument, "bad channel id")
	}

	ctx, cancelFunc := context.WithCancel(call.Context())
	defer cancelFunc()

	reliable := req.Reliable
	serverID := p.serverID.String()

	oldChannel, newChannel, err := p.modelController.AcquireChannel(ctx,
		channelID, serverID, reliable)
	if err != nil {
		return err
	}

	log.Print("got channel")
	spew.Dump(newChannel)

	if oldChannel.ServerID != "" {
		// TODO: Evict the user from the other server.
	}

	// Subscribe after acquire so we don't evict ourselves.
	internalChannel := p.router.Subscribe(channelID)
	ackList := make(map[string]router.Message)
	defer func() {
		done := p.router.Unsubscribe(channelID, internalChannel)
		for _, msg := range ackList {
			msg.Result <- context.Canceled
		}

		for {
			select {
			case <-done:
				return
			case msg, ok := <-internalChannel:
				if ok == false {
					return
				}
				msg.Result <- context.Canceled
			}
		}
	}()

	handleMessage := func(msg router.Message) error {
		switch obj := msg.Obj.(type) {
		case *pb.InternalEvictRequest:
			msg.Result <- nil
			return context.Canceled
		case *pb.InternalAckRequest:
			// TODO: Handle JWT AckIDs
			ack, ok := ackList[obj.AckId]
			if ok {
				ack.Result <- nil
				delete(ackList, obj.AckId)
			}
			msg.Result <- nil
		case *pb.InternalPublishRequest:
			if reliable && obj.Reliable {
				ackID := uuid.New().String()
				// TODO: Generate JWT as AckID
				err = call.Send(&pb.StreamResponse{
					Id:       obj.Id,
					Ts:       obj.Ts,
					Message:  obj.Message,
					Reliable: true,
					TopicId:  obj.TopicId,
					AckId:    ackID,
				})
				if err != nil {
					msg.Result <- err
					return err
				}
				ackList[ackID] = msg
			} else {
				err = call.Send(&pb.StreamResponse{
					Id:       obj.Id,
					Ts:       obj.Ts,
					Message:  obj.Message,
					Reliable: false,
					TopicId:  obj.TopicId,
				})
				if err != nil {
					msg.Result <- err
					return err
				}
				msg.Result <- nil
			}
		}
		return ErrInvalidMessage
	}

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		// TODO: Handle pulling from Queue/Acking queue
		case msg := <-internalChannel:
			err = handleMessage(msg)
			if err != nil {
				return err
			}
		case <-time.After(5 * time.Second): // TODO: Get poll time from config
			clk, err := p.modelController.GetChannelClock(ctx, channelID)
			if err != nil {
				return err
			}

			if clk > newChannel.Clock {
				return status.Error(codes.Aborted, "channel evicted")
			}
		}
	}
}
