package main

import (
	"context"
	"log"
	"math/rand"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

var (
	ErrNodeNotFound   = status.Error(codes.NotFound, "node not found")
	ErrBadTopicID     = status.Error(codes.InvalidArgument, "bad topic id")
	ErrBadRangeBegin  = status.Error(codes.InvalidArgument, "bad range begin")
	ErrBadRangeEnd    = status.Error(codes.InvalidArgument, "bad end begin")
	ErrNoNodes        = status.Error(codes.Unavailable, "no nodes")
	ErrRangesAreEqual = status.Error(codes.InvalidArgument, "uuids are equal")
	ErrBadRangeWidth  = status.Error(codes.InvalidArgument, "bad range width")
)

func partitionUUID(a uuid.UUID, b uuid.UUID) (left uuid.UUID, right uuid.UUID, ok bool) {
	left = uuid.UUID{
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff}
	right = uuid.UUID{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}

	for i, aa := range a {
		bb := b[i]
		if aa == bb {
			left[i], right[i] = aa, aa
			continue
		}

		d := bb - aa
		h := aa + d/2
		left[i] = h
		right[i] = h + 1
		ok = true
		return
	}

	ok = false
	return
}

func (p *Provider) Distribute(ctx context.Context,
	req *pb.DistributeRequest) (*pb.DistributeResponse, error) {
	var err error
	if req.RangeWidth == 0 {
		return nil, ErrBadRangeWidth
	}

	begin, err := uuid.Parse("00000000-0000-0000-0000-000000000000")
	if err != nil {
		return nil, ErrBadRangeBegin
	}

	end, err := uuid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	if err != nil {
		return nil, ErrBadRangeEnd
	}

	replicators := p.planners.GetList()
	if len(replicators) == 0 {
		return nil, ErrNoNodes
	}

	minToDivide := 500 // TODO: Configure
	rangeWidth := int(req.RangeWidth)

	pairs := []uuid.UUID{begin, end}
	partitions := 1
	for rangeWidth > minToDivide {
		/*if partitions*2 > len(replicators) {
			break
		}*/
		rangeWidth = rangeWidth / 2
		partitions = partitions * 2

		newPairs := make([]uuid.UUID, len(pairs)*2)
		for i := 0; i < len(pairs); i += 2 {
			var ok bool
			newPairs[i*2], newPairs[i*2+3] = pairs[i], pairs[i+1]
			newPairs[i*2+1], newPairs[i*2+2], ok = partitionUUID(pairs[i], pairs[i+1])
			if ok == false {
				return nil, ErrRangesAreEqual
			}
		}
		pairs = newPairs
	}

	process := func(rangeBegin uuid.UUID, rangeEnd uuid.UUID) error {
		address := replicators[rand.Int()%len(replicators)]
		conn, err := p.grpcClients.Connect(address)
		service := pb.NewPlanServiceClient(conn)
		rq := pb.PlanRequest{
			Id:         req.Id,
			Message:    req.Message,
			Reliable:   req.Reliable,
			TopicId:    req.TopicId,
			Ts:         req.Ts,
			RangeBegin: rangeBegin.String(),
			RangeEnd:   rangeEnd.String(),
			RangeWidth: int32(rangeWidth),
		}
		_, err = service.Plan(ctx, &rq)
		if err != nil && req.Reliable == false {
			// TODO: Handle non-critical error
			log.Print(err)
			return nil
		} else if err != nil {
			return err
		}
		return nil
	}

	if len(pairs) == 2 {
		err = process(pairs[0], pairs[1])
		if err != nil {
			return nil, err
		}
	} else {
		wg := sync.WaitGroup{}
		errMut := sync.Mutex{}
		var errRes error

		for i := 0; i < len(pairs); i += 2 {
			rangeBegin := pairs[i]
			rangeEnd := pairs[i+1]

			// TODO: Move off to worker routines.
			wg.Add(1)
			err = p.plannerDispatchWorkerPool.Push(func() {
				defer wg.Done()
				err := process(rangeBegin, rangeEnd)
				if err != nil {
					errMut.Lock()
					errRes = multierror.Append(errRes, err)
					errMut.Unlock()
				}
			})
			if err != nil {
				wg.Done()
				return nil, err
			}
		}

		wg.Wait()

		if errRes != nil {
			return nil, errRes
		}
	}

	return &pb.DistributeResponse{}, nil
}
