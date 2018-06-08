package main

import (
	"log"
	"sync"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) InternalPublish(req *pb.InternalPublishRequest,
	call pb.SubscribeInternalService_InternalPublishServer) error {
	ctx := call.Context()
	reliable := req.Message.GetReliable()

	wg := sync.WaitGroup{}
	sendMutex := sync.Mutex{}

	process := func(channelID string) {
		defer wg.Done()
		err := p.router.Publish(ctx, channelID, req.Message)
		if reliable == false {
			return
		}

		if err != nil {
			// TODO: Handle error messages
			log.Print(err)
		}

		// Ignore errors if they are unreliable messages.
		sendMutex.Lock()
		err = call.Send(&pb.InternalPublishResponse{
			ChannelId: channelID,
			Success:   err == nil,
		})
		sendMutex.Unlock()

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
