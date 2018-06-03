package main

import (
	"context"
	"fmt"
	"time"

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

	rq := pb.InternalAckRequest{
		AckId:     req.AckId,
		ChannelId: req.ChannelId,
	}

	if serverID == p.serverID {
		err = p.router.Publish(timeoutCtx, req.ChannelId, &rq)
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

	url := fmt.Sprintf("https://%s", address)
	rc := pb.NewSubscribeInternalServiceProtobufClient(url, p.quicClient)
	_, err = rc.InternalAck(timeoutCtx, &rq)
	if err != nil {
		return nil, err
	}

	return &pb.AckResponse{}, nil
}
