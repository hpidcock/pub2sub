package main

import (
	"context"
	"time"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) InternalPublish(ctx context.Context,
	req *pb.InternalPublishRequest) (*pb.InternalPublishResponse, error) {
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	spew.Dump(req)

	err := p.router.Publish(timeoutCtx, req.ChannelId, req)
	if err != nil {
		return nil, err
	}

	return &pb.InternalPublishResponse{}, nil
}
