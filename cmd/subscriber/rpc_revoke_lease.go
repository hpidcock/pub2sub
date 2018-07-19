package main

import (
	"context"
	"time"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/struuid"
)

const (
	RevokeLimit = 32
)

func (p *Provider) RevokeLease(ctx context.Context,
	req *pb.RevokeLeaseRequest) (*pb.RevokeLeaseResponse, error) {
	channelID, err := struuid.Parse(req.ChannelId)
	if err != nil {
		return nil, ErrFailedToParseChannelID
	}

	topicID, err := struuid.Parse(req.TopicId)
	if err != nil {
		return nil, ErrFailedToParseTopicID
	}

	upTo := time.Now()
	exists := true

	for i := 0; i < RevokeLimit && exists; i++ {
		upTo, exists, err = p.topicController.Unsubscribe(ctx, topicID, upTo, channelID)
		if err != nil {
			return nil, err
		}
	}

	return &pb.RevokeLeaseResponse{}, nil
}
