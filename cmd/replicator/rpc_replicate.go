package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hpidcock/pub2sub/pkg/channel"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/topic"
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

func (p *Provider) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if p.config.TerminationLayer {
		return p.replicateForward(ctx, req)
	} else {
		return p.replicateDown(ctx, req)
	}
}

func (p *Provider) replicateForward(ctx context.Context,
	req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	topicID, err := uuid.Parse(req.TopicId)
	if err != nil {
		return nil, ErrBadTopicID
	}

	begin, err := uuid.Parse(req.RangeBegin)
	if err != nil {
		return nil, ErrBadRangeBegin
	}

	end, err := uuid.Parse(req.RangeEnd)
	if err != nil {
		return nil, ErrBadRangeEnd
	}

	channelIDs, err := p.topicController.ScanTopic(ctx, topicID, time.Now(), begin, end)
	if err != nil {
		return nil, err
	}

	if len(channelIDs) == 0 {
		return &pb.ReplicateResponse{}, nil
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for _, v := range channelIDs {
		channelID := v
		eg.Go(func() error {
			return p.forwardMessage(egCtx, channelID, req)
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return &pb.ReplicateResponse{}, nil
}

func (p *Provider) replicateDown(ctx context.Context,
	req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	var err error
	if req.RangeWidth == 0 {
		return nil, ErrBadRangeWidth
	}

	begin, err := uuid.Parse(req.RangeBegin)
	if err != nil {
		return nil, ErrBadRangeBegin
	}

	end, err := uuid.Parse(req.RangeEnd)
	if err != nil {
		return nil, ErrBadRangeEnd
	}

	replicators := p.nextLayer.GetList()
	if len(replicators) == 0 {
		return nil, ErrNoNodes
	}

	minToDivide := 100 // TODO: Configure
	rangeWidth := int(req.RangeWidth)

	pairs := []uuid.UUID{begin, end}
	partitions := 1
	for rangeWidth > minToDivide {
		if partitions*2 > len(replicators) {
			break
		}
		rangeWidth = rangeWidth / 2
		partitions = partitions * 2

		newPairs := make([]uuid.UUID, len(pairs)*2)
		for i := 0; i < len(pairs); i += 2 {
			var ok bool
			newPairs[i*2], newPairs[i*2+1], ok = partitionUUID(pairs[i], pairs[i+1])
			if ok == false {
				return nil, ErrRangesAreEqual
			}
		}
	}

	// TODO: Reuse go routines or use worker routines.
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < len(pairs); i += 2 {
		rangeBegin := pairs[i]
		rangeEnd := pairs[i+1]

		eg.Go(func() error {
			address := replicators[rand.Int()%len(replicators)]
			url := fmt.Sprintf("https://%s", address)
			rc := pb.NewReplicationServiceProtobufClient(url, p.quicClient)
			rq := pb.ReplicateRequest{
				Id:         req.Id,
				Message:    req.Message,
				Reliable:   req.Reliable,
				TopicId:    req.TopicId,
				Ts:         req.Ts,
				RangeBegin: rangeBegin.String(),
				RangeEnd:   rangeEnd.String(),
				RangeWidth: int32(rangeWidth),
			}
			_, err = rc.Replicate(egCtx, &rq)
			if err != nil && req.Reliable == false {
				// TODO: Handle non-critical error
				log.Print(err)
				return nil
			} else if err != nil {
				return err
			}
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return &pb.ReplicateResponse{}, nil
}

func (p *Provider) forwardMessage(ctx context.Context, channelID uuid.UUID,
	req *pb.ReplicateRequest) (errOut error) {
	reliable := req.Reliable
	if reliable == false {
		defer func() {
			if errOut != nil {
				log.Println("dropping unreliable message: ", errOut)
			}
			// Snuff errors for unreliable messages.
			errOut = nil
		}()
	}

	// TODO: configure
	ctx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	noChannel := false
	serverID, err := p.channelClient.GetChannelServerID(ctx, channelID)
	if err == channel.ErrChannelNotFound {
		noChannel = true
	} else if err != nil {
		return err
	}

	if noChannel && reliable == false {
		log.Println("dropping unreliable message: no channel")
		return nil
	}

	if req.Reliable {
		defer func() {
			if errOut == nil {
				return
			}

			channelMessage := pb.ChannelMessage{
				Id:      req.Id,
				Message: req.Message,
				TopicId: req.TopicId,
				Ts:      req.Ts,
			}

			payload, err := proto.Marshal(&channelMessage)
			if err != nil {
				errOut = multierror.Append(errOut, err)
				return
			}

			_, err = p.topicController.PushMessage(ctx, channelID, payload)
			if err == topic.ErrQueueNotFound {
				log.Println("dropping reliable message: no queue")
				// Either the queue has expired or the channel is unreliable.
			} else if err != nil {
				errOut = multierror.Append(errOut, err)
				return
			}

			// Error was handled by pushing to the queue, if it exists.
			errOut = nil
		}()
	}

	rq := pb.InternalPublishRequest{
		ChannelId: channelID.String(),
		Id:        req.Id,
		Message:   req.Message,
		Reliable:  req.Reliable,
		TopicId:   req.TopicId,
		Ts:        req.Ts,
	}

	subscriberNodes := p.subscriberLayer.GetMap()
	address, ok := subscriberNodes[serverID]
	if ok == false {
		return ErrNodeNotFound
	}

	url := fmt.Sprintf("https://%s", address)
	rc := pb.NewSubscribeInternalServiceProtobufClient(url, p.quicClient)

	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	_, err = rc.InternalPublish(timeoutCtx, &rq)
	if err != nil {
		return err
	}

	return nil
}
