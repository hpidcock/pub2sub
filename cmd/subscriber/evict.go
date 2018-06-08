package main

import (
	"context"

	"github.com/hpidcock/go-pub-sub-channel"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) evict(ctx context.Context,
	serverID uuid.UUID, channelID uuid.UUID) error {
	var err error

	channelIDString := channelID.String()
	if serverID == p.serverID {
		err = p.router.Publish(ctx, channelIDString, &pb.InternalEvictMessage{
			ChannelId: channelIDString,
		})
		if err == router.ErrNotDelivered {
		} else if err != nil {
			return err
		}
		return nil
	}

	subscriberNodes := p.subscribers.GetMap()
	address, ok := subscriberNodes[serverID]
	if ok == false {
		return status.Error(codes.NotFound, "node not found")
	}

	msg := &pb.UDPUnreliableMessage{
		Type:      pb.UDPMessageType_EVICT,
		ChannelId: channelIDString,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Best effort.
	err = p.udpClient.Send(address, data)
	if err != nil {
		return err
	}

	return nil
}
