package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
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

	topicIDs := make([]uuid.UUID, len(req.TopicIds))
	for k, v := range req.TopicIds {
		topicID, err := uuid.Parse(v)
		if err != nil {
			return nil, err
		}
		topicIDs[k] = topicID
	}

	// TODO: Use req.Ts for correct subscriber list within a resonable grace period.
	topicWidths, err := p.topicController.GetTopicsWidth(ctx, topicIDs, time.Now())
	if err != nil {
		return nil, err
	}

	totalSubscribers := funk.SumInt64(topicWidths)
	if totalSubscribers == 0 {
		return &pb.PublishResponse{}, nil
	}

	distributors := p.distributors.GetList()
	for k, topicID := range req.TopicIds {
		width := topicWidths[k]
		if width == 0 {
			continue
		}

		// TODO: Move off to worker routines.
		address := distributors[rand.Int()%len(distributors)]
		conn, err := p.grpcClients.Connect(address)
		service := pb.NewDistributeServiceClient(conn)

		rq := pb.DistributeRequest{
			Id:         req.Id,
			Message:    req.Message,
			Reliable:   req.Reliable,
			TopicId:    topicID,
			Ts:         req.Ts,
			RangeWidth: int32(width),
		}
		_, err = service.Distribute(ctx, &rq)
		if err != nil && req.Reliable == false {
			log.Print(err)
			continue
		} else if err != nil {
			return nil, err
		}
	}

	return &pb.PublishResponse{}, nil
}
