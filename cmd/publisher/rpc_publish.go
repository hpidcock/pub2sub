package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	return nil, status.Error(codes.Unimplemented, "nope")
}
