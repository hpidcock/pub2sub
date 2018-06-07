package main

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

var (
	numCPUs = runtime.NumCPU()
)

func (p *Provider) Plan(ctx context.Context,
	req *pb.PlanRequest) (*pb.PlanResponse, error) {
	reliable := req.Reliable

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

	numChannels := len(channelIDs)
	if numChannels == 0 {
		return &pb.PlanResponse{}, nil
	}

	var wg sync.WaitGroup
	var offlineMutex sync.Mutex
	var offline []uuid.UUID

	var groupsMutex sync.Mutex
	groups := make(map[uuid.UUID][]uuid.UUID)

	workers := numCPUs * 32
	if numChannels < workers {
		workers = numChannels
	}

	wg.Add(workers)
	for n := 0; n < workers; n++ {
		go func(n int) {
			defer wg.Done()
			for i := n; i < numChannels; i += workers {
				channelID := channelIDs[i]

				noChannel := false
				serverID, err := p.channelClient.GetChannelServerID(ctx, channelID)
				if err != nil {
					noChannel = true
				}

				if noChannel && reliable == false {
					return
				}

				if noChannel {
					offlineMutex.Unlock()
					offline = append(offline, channelID)
					offlineMutex.Unlock()
				} else {
					groupsMutex.Lock()
					group, _ := groups[serverID]
					groups[serverID] = append(group, channelID)
					groupsMutex.Unlock()
				}
			}
		}(n)
	}
	wg.Wait()

	eg, egCtx := errgroup.WithContext(ctx)
	numOffline := len(offline)
	if numOffline > 0 {
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

		offlineWorkers := numCPUs * 32
		if numOffline < offlineWorkers {
			offlineWorkers = numOffline
		}

		for x := 0; x < offlineWorkers; x++ {
			n := x
			eg.Go(func() error {
				for i := n; i < numOffline; i += offlineWorkers {
					channelID := offline[i]
					_, err := p.topicController.PushMessage(egCtx, channelID, payload)
					if err == topic.ErrQueueNotFound {
						log.Println("dropping reliable message: no queue")
						// Either the queue has expired or the channel is unreliable.
					} else if err != nil {
						return err
					}
				}
				return nil
			})
		}
	}

	executorNodes := p.executors.GetMap()
	executorNodesStr := make(map[string]string)
	for k, v := range executorNodes {
		executorNodesStr[k.String()] = v
	}

	for serverUUID, channelUUIDs := range groups {
		serverID := serverUUID.String()
		channelIDs := make([]string, len(channelUUIDs))
		for i, channelUUID := range channelUUIDs {
			channelIDs[i] = channelUUID.String()
		}
		eg.Go(func() error {
			targetExecutor := ""
			address := ""
			// Somewhat stable rendezvous mapping.
			for executorNodeID, v := range executorNodesStr {
				if executorNodeID > serverID {
					targetExecutor = executorNodeID
					address = v
				}
			}

			if targetExecutor == "" || address == "" {
				log.Fatal("bad state")
			}

			conn, err := p.grpcClients.Connect(address)
			if err != nil {
				return err
			}

			pub := &pb.ExecuteRequest{
				ChannelIds: channelIDs,
				Id:         req.Id,
				Message:    req.Message,
				Reliable:   req.Reliable,
				TopicId:    req.TopicId,
				Ts:         req.Ts,
			}

			service := pb.NewExecuteServiceClient(conn)
			timeoutCtx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
			defer cancelFunc()
			_, err = service.Execute(timeoutCtx, pub)
			if err != nil {
				return err
			}

			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return &pb.PlanResponse{}, nil
}
