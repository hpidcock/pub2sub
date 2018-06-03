package main

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) evict(ctx context.Context,
	serverID uuid.UUID, channelID uuid.UUID) error {
	var err error
	rq := pb.InternalEvictRequest{
		ChannelId: channelID.String(),
	}

	if serverID == p.serverID {
		err = p.router.Publish(ctx, rq.ChannelId, &rq)
		if err != nil {
			return err
		}

		return nil
	}

	subscriberNodes := p.subscribers.GetMap()
	address, ok := subscriberNodes[serverID]
	if ok == false {
		return status.Error(codes.NotFound, "node not found")
	}

	url := fmt.Sprintf("https://%s", address)
	rc := pb.NewSubscribeInternalServiceProtobufClient(url, p.quicClient)
	_, err = rc.InternalEvict(ctx, &rq)
	if err != nil {
		return err
	}

	return nil
}
