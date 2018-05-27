package main

import (
	"context"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Ack(ctx context.Context,
	req *pb.AckRequest) (*pb.AckResponse, error) {

	// TODO: Handle forwarding ack.

	return &pb.AckResponse{}, nil
}
