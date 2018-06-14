package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/struuid"
	"github.com/hpidcock/pub2sub/pkg/topic"
)

var (
	ErrFailedToParseChannelID = status.Error(codes.InvalidArgument, "failed to parse channel id")
	ErrFailedToParseTopicID   = status.Error(codes.InvalidArgument, "failed to parse topic id")
	ErrFailedToAcquireLease   = status.Error(codes.Internal, "failed to acquire lease")
)

func (p *Provider) Lease(ctx context.Context,
	req *pb.LeaseRequest) (*pb.LeaseResponse, error) {
	start := time.Now()

	channelID, err := struuid.Parse(req.ChannelId)
	if err != nil {
		return nil, ErrFailedToParseChannelID
	}

	topicID, err := struuid.Parse(req.TopicId)
	if err != nil {
		return nil, ErrFailedToParseTopicID
	}

	expireDuration := time.Duration(req.ExpireIn) * time.Second
	if p.config.MaxSubscribeDuration < expireDuration {
		expireDuration = p.config.MaxSubscribeDuration
	}

	asOf := start
	target := asOf.Add(expireDuration)
	didAlreadyExist := false

	for asOf.Before(target) {
		upTo, exists, err := p.topicController.Subscribe(ctx, topicID, asOf, channelID)
		if err != nil {
			log.Print(err)
			// TODO: Handle error
			break
		}

		didAlreadyExist = didAlreadyExist || exists
		asOf = upTo
	}

	if asOf.After(start) == false {
		return nil, ErrFailedToAcquireLease
	}

	leaseDuration := asOf.Sub(start)
	err = p.topicController.ExtendQueue(ctx, channelID, leaseDuration)
	if err == topic.ErrQueueNotFound {
	} else if err != nil {
		return nil, err
	}

	res := &pb.LeaseResponse{
		Ttl:    int64(leaseDuration.Seconds()),
		Exists: didAlreadyExist,
	}
	return res, nil
}
