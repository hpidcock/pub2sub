package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/hashicorp/go-multierror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/topic"
)

var (
	ErrNodeNotFound = status.Error(codes.NotFound, "node not found")
)

func (p *Provider) Execute(ctx context.Context,
	req *pb.ExecuteRequest) (res *pb.ExecuteResponse, errOut error) {
	reliable := req.Reliable
	var unhandled map[uuid.UUID]bool
	if reliable {
		unhandled = make(map[uuid.UUID]bool)
		for _, channelIDString := range req.ChannelIds {
			channelID, err := uuid.Parse(channelIDString)
			if err != nil {
				// TODO: Handle error
				continue
			}
			unhandled[channelID] = true
		}

		defer func() {
			if errOut == nil {
				return
			}

			channelMessage := pb.ChannelMessage{
				Id:      req.Id,
				Message: req.Message,
				TopicId: req.TopicId,
				Ts:      req.Ts,
			}

			payload, err := proto.Marshal(&channelMessage)
			if err != nil {
				errOut = multierror.Append(errOut, err)
				return
			}

			var newErr error
			for channelID, errored := range unhandled {
				if errored == false {
					continue
				}

				log.Print(errOut)
				log.Print("queing")

				// TODO: Handle parallel inserts.
				_, err = p.topicController.PushMessage(ctx, channelID, payload)
				if err == topic.ErrQueueNotFound {
					// Either the queue has expired or the channel is unreliable.
					log.Println("dropping reliable message: no queue")
				} else if err != nil {
					newErr = multierror.Append(newErr, err)
				}
			}

			if newErr != nil {
				errOut = newErr
				return
			}

			// Error was handled by pushing to the queue, if it exists.
			errOut = nil
			res = &pb.ExecuteResponse{}
		}()
	} else {
		defer func() {
			if errOut != nil {
				log.Println("dropping unreliable message: ", errOut)
			}
			// Snuff errors for unreliable messages.
			errOut = nil
			res = &pb.ExecuteResponse{}
		}()
	}

	serverID, err := uuid.Parse(req.ServerId)
	if err != nil {
		return nil, err
	}

	subscriberNodes := p.subscribers.GetMap()
	address, ok := subscriberNodes[serverID]
	if ok == false {
		return nil, ErrNodeNotFound
	}

	conn, err := p.grpcClients.Connect(address)
	if err != nil {
		return nil, err
	}

	pub := &pb.InternalPublishRequest{
		ChannelId: req.ChannelIds,
		Message: &pb.InternalPublishMessage{
			Id:       req.Id,
			Message:  req.Message,
			Reliable: req.Reliable,
			TopicId:  req.TopicId,
			Ts:       req.Ts,
		},
	}

	service := pb.NewSubscribeInternalServiceClient(conn)
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	call, err := service.InternalPublish(timeoutCtx, pub)
	if err != nil {
		return nil, err
	}

	for {
		resMsg, err := call.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		channelID, err := uuid.Parse(resMsg.ChannelId)
		if err != nil {
			// TODO: handle error, for now, it's as if it failed to send.
			log.Print(err)
			continue
		}

		unhandled[channelID] = false
	}

	return &pb.ExecuteResponse{}, nil
}
