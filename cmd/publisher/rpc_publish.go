package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/google/uuid"
	"github.com/thoas/go-funk"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) Publish(ctx context.Context,
	req *pb.PublishRequest) (*pb.PublishResponse, error) {
	// TODO: Validate request.
	var err error
	topicCount := len(req.TopicIds)
	if topicCount == 0 {
		return &pb.PublishResponse{}, nil
	}

	topicIDs := make([]uuid.UUID, topicCount)
	for k, v := range req.TopicIds {
		topicID, err := uuid.Parse(v)
		if err != nil {
			return nil, err
		}
		topicIDs[k] = topicID
	}

	topicWidths, err := p.topicController.GetTopicsWidth(ctx, topicIDs, time.Now())
	if err != nil {
		return nil, err
	}

	totalSubscribers := funk.SumInt64(topicWidths)
	if totalSubscribers == 0 {
		return &pb.PublishResponse{}, nil
	}

	distributors := p.distributors.GetList()
	process := func(topicID string, width int64) error {
		address := distributors[rand.Int()%len(distributors)]
		conn, err := p.grpcClients.Connect(address)
		if err != nil {
			return err
		}
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
		} else if err != nil {
			return err
		}

		return nil
	}

	if topicCount == 1 {
		err := process(req.TopicIds[0], topicWidths[0])
		if err != nil {
			return nil, err
		}
	} else {
		wg := sync.WaitGroup{}
		errMut := sync.Mutex{}
		var errRes error
		for k, v := range req.TopicIds {
			width := topicWidths[k]
			if width == 0 {
				continue
			}
			topicID := v

			wg.Add(1)
			err = p.publishWorkerPool.Push(func() {
				defer wg.Done()
				err := process(topicID, width)
				if err != nil {
					errMut.Lock()
					errRes = multierror.Append(errRes, err)
					errMut.Unlock()
				}
			})
			if err != nil {
				wg.Done()
			}
		}

		wg.Wait()
		if errRes != nil {
			return nil, errRes
		}
	}

	return &pb.PublishResponse{}, nil
}
