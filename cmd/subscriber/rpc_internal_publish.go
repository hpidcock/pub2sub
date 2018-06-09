package main

import (
	"log"
	"sync"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) InternalPublish(req *pb.InternalPublishRequest,
	call pb.SubscribeInternalService_InternalPublishServer) error {
	ctx := call.Context()
	count := len(req.ChannelId)
	reliable := req.Message.GetReliable()
	if count == 0 {
		return nil
	}

	sendMutex := sync.Mutex{}
	process := func(channelID string) {
		err := p.router.Publish(ctx, channelID, req.Message)
		if reliable == false {
			return
		}

		if err != nil {
			// TODO: Handle error messages
			log.Print("internal publish error: ", err)
		}

		// Ignore errors if they are unreliable messages.
		sendMutex.Lock()
		err = call.Send(&pb.InternalPublishResponse{
			ChannelId: channelID,
			Success:   err == nil,
		})
		sendMutex.Unlock()

		if err != nil {
			// TODO: handle error returned by Send
			log.Print("send internal publish response error: ", err)
		}
	}

	if count == 1 {
		// Just use this routine.
		process(req.ChannelId[0])
	} else {
		wg := sync.WaitGroup{}
		wg.Add(count)
		for _, v := range req.ChannelId {
			channelID := v
			err := p.publishWorkerPool.Push(func() {
				process(channelID)
				wg.Done()
			})
			if err != nil {
				wg.Done()
			}
		}
		wg.Wait()
	}

	return nil
}
