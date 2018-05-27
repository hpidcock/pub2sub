package main

import (
	"context"
	"time"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) InternalEvict(ctx context.Context,
	req *pb.InternalEvictRequest) (*pb.InternalEvictResponse, error) {
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	err := p.router.Publish(timeoutCtx, req.ChannelId, req)
	if err != nil {
		return nil, err
	}

	return &pb.InternalEvictResponse{}, nil
}
