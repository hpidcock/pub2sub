package main

import (
	"context"
	"log"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/struuid"
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

var (
	numCPUs = runtime.NumCPU()
)

func (p *Provider) Plan(ctx context.Context,
	req *pb.PlanRequest) (*pb.PlanResponse, error) {
	reliable := req.Reliable

	topicID, err := struuid.Parse(req.TopicId)
	if err != nil {
		return nil, ErrBadTopicID
	}

	begin, err := struuid.Parse(req.RangeBegin)
	if err != nil {
		return nil, ErrBadRangeBegin
	}

	end, err := struuid.Parse(req.RangeEnd)
	if err != nil {
		return nil, ErrBadRangeEnd
	}

	executorNodes := p.executors.GetMap()
	if len(executorNodes) == 0 {
		return nil, ErrNoNodes
	}

	channelIDs, err := p.topicController.ScanTopic(ctx, topicID, time.Now(), begin, end)
	if err != nil {
		return nil, err
	}

	numChannels := len(channelIDs)
	if numChannels == 0 {
		return &pb.PlanResponse{}, nil
	}

	found, offline, err := p.channelClient.BatchGetChannelServerID(ctx, channelIDs)
	if err != nil {
		return nil, err
	}

	groups := make(map[struuid.UUID][]struuid.UUID)
	for channelID, serverID := range found {
		group, _ := groups[serverID]
		groups[serverID] = append(group, channelID)
	}

	if reliable && len(offline) > 0 {
		channelMessage := pb.ChannelMessage{
			Id:      req.Id,
			Message: req.Message,
			TopicId: req.TopicId,
			Ts:      req.Ts,
		}

		payload, err := proto.Marshal(&channelMessage)
		if err != nil {
			return nil, err
		}

		wg := sync.WaitGroup{}
		mut := sync.Mutex{}
		var errOut error

		for _, v := range offline {
			channelID := v
			wg.Add(1)
			err = p.offlineQueueWorkerPool.Push(func() {
				defer wg.Done()
				_, err := p.topicController.PushMessage(ctx, channelID, payload)
				if err == topic.ErrQueueNotFound {
					// Either the queue has expired or the channel is unreliable.
					log.Println("dropping reliable message: no queue")
				} else if err != nil {
					mut.Lock()
					errOut = multierror.Append(errOut, err)
					mut.Unlock()
				}
			})
			if err != nil {
				wg.Done()
				return nil, err
			}
		}

		wg.Wait()

		if errOut != nil {
			return nil, errOut
		}
	}

	if len(groups) == 0 {
		return &pb.PlanResponse{}, nil
	}

	executorNodesStr := make(map[string]string)
	var executors []string
	for executorUUID, v := range executorNodes {
		executorID := executorUUID.String()
		executorNodesStr[executorID] = v
		executors = append(executors, executorID)
	}
	sort.Strings(executors)

	wg := sync.WaitGroup{}
	mut := sync.Mutex{}
	var errOut error

	for serverUUID, channelUUIDs := range groups {
		serverID := serverUUID.String()
		channelIDs := make([]string, len(channelUUIDs))
		for i, channelUUID := range channelUUIDs {
			channelIDs[i] = channelUUID.String()
		}

		wg.Add(1)
		err = p.executorDispatchWorkerPool.Push(func() {
			defer wg.Done()
			targetExecutor := executors[0]
			// Somewhat stable rendezvous mapping.
			for _, executorID := range executors {
				if executorID > serverID {
					targetExecutor = executorID
					break
				}
			}
			address := executorNodesStr[targetExecutor]

			conn, err := p.grpcClients.Connect(address)
			if err != nil {
				mut.Lock()
				errOut = multierror.Append(errOut, err)
				mut.Unlock()
			}

			pub := &pb.ExecuteRequest{
				ChannelIds: channelIDs,
				Id:         req.Id,
				Message:    req.Message,
				Reliable:   req.Reliable,
				TopicId:    req.TopicId,
				Ts:         req.Ts,
				ServerId:   serverID,
			}

			service := pb.NewExecuteServiceClient(conn)
			timeoutCtx, cancelFunc := context.WithTimeout(ctx, 60*time.Second)
			defer cancelFunc()
			_, err = service.Execute(timeoutCtx, pub)
			if err != nil {
				mut.Lock()
				errOut = multierror.Append(errOut, err)
				mut.Unlock()
			}
		})

		if err != nil {
			wg.Done()
			return nil, err
		}
	}

	wg.Wait()

	if errOut != nil {
		return nil, errOut
	}

	return &pb.PlanResponse{}, nil
}
