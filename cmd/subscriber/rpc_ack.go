package main

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) Ack(ctx context.Context,
	req *pb.AckRequest) (*pb.AckResponse, error) {
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	channelID, err := uuid.Parse(req.ChannelId)
	if err != nil {
		return nil, err
	}

	serverID, err := p.channelClient.GetChannelServerID(ctx, channelID)
	if err != nil {
		return nil, err
	}

	if serverID == p.serverID {
		err = p.router.Publish(timeoutCtx, req.ChannelId, &pb.InternalAckMessage{
			AckId:     req.AckId,
			ChannelId: req.ChannelId,
		})
		if err != nil {
			return nil, err
		}

		return &pb.AckResponse{}, nil
	}

	subscriberNodes := p.subscribers.GetMap()
	address, ok := subscriberNodes[serverID]
	if ok == false {
		return nil, status.Error(codes.NotFound, "node not found")
	}

	msg := &pb.UDPUnreliableMessage{
		Type:      pb.UDPMessageType_ACK,
		ChannelId: req.ChannelId,
		AckId:     req.AckId,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Best effort.
	err = p.udpClient.Send(address, data)
	if err != nil {
		return nil, err
	}

	return &pb.AckResponse{}, nil
}
