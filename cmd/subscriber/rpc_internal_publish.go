package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) InternalPublish(req *pb.InternalPublishRequest,
	call pb.SubscribeInternalService_InternalPublishServer) error {
	ctx := call.Context()
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	reliable := req.Message.GetReliable()

	wg := sync.WaitGroup{}
	process := func(channelID string) {
		defer wg.Done()
		err := p.router.Publish(timeoutCtx, channelID, req.Message)
		if reliable == false {
			return
		}

		if err != nil {
			// TODO: Handle error messages
			log.Print(err)
		}

		// Ignore errors if they are unreliable messages.
		err = call.Send(&pb.InternalPublishResponse{
			ChannelId: channelID,
			Success:   err == nil,
		})

		if err != nil {
			log.Print(err)
			// TODO: handle error returned by Send
		}
	}

	wg.Add(len(req.ChannelId))
	for _, channelID := range req.ChannelId {
		// TODO: use worker threads.
		go process(channelID)
	}

	wg.Wait()

	return nil
}
