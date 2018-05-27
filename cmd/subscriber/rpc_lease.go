package main

import (
	"context"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Lease(ctx context.Context,
	req *pb.LeaseRequest) (*pb.LeaseResponse, error) {

	return &pb.LeaseResponse{}, nil
}
