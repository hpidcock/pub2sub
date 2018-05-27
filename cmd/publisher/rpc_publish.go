package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/thoas/go-funk"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Publish(ctx context.Context,
	req *pb.PublishRequest) (*pb.PublishResponse, error) {
	// TODO: Validate request.
	var err error
	if req.TopicIds == nil || len(req.TopicIds) == 0 {
		return &pb.PublishResponse{}, nil
	}

	topicWidths, err := p.modelController.GetTopicsWidth(ctx, req.TopicIds)
	if err != nil {
		return nil, err
	}

	totalSubscribers := funk.SumInt64(topicWidths)
	if totalSubscribers == 0 {
		return &pb.PublishResponse{}, nil
	}

	replicators := p.replicators.GetList()
	for k, topicID := range req.TopicIds {
		width := topicWidths[k]
		if width == 0 {
			continue
		}

		address := replicators[rand.Int()%len(replicators)]
		url := fmt.Sprintf("https://%s", address)
		rc := pb.NewReplicationServiceProtobufClient(url, p.quicClient)

		rq := pb.ReplicateRequest{
			Id:         req.Id,
			Message:    req.Message,
			Reliable:   req.Reliable,
			TopicId:    topicID,
			Ts:         req.Ts,
			RangeBegin: "00000000-0000-0000-0000-000000000000",
			RangeEnd:   "ffffffff-ffff-ffff-ffff-ffffffffffff",
			RangeWidth: int32(width),
		}
		_, err = rc.Replicate(ctx, &rq)
		if err != nil {
			return nil, err
		}
	}

	return &pb.PublishResponse{}, nil
}
