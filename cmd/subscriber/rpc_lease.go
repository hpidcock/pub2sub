package main

import (
	"context"
	"time"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Lease(ctx context.Context,
	req *pb.LeaseRequest) (*pb.LeaseResponse, error) {
	// TODO: Finish
	expireAt := time.Now().UTC().Add(time.Duration(req.ExpireIn) * time.Second)
	err := p.modelController.Subscribe(ctx, req.ChannelId,
		req.TopicId, expireAt)
	if err != nil {
		return nil, err
	}

	return &pb.LeaseResponse{}, nil
}
