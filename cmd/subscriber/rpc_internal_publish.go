package main

import (
	"log"

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

	channelIDs := req.ChannelId
	chanList := make([]<-chan error, count)
	for k, channelID := range channelIDs {
		chanList[k] = p.router.NonBlockingPublish(ctx, channelID, req.Message)
	}

	batchIDs := [8]string{}
	batch := [8]<-chan error{}
	for i := 0; i < 8 && i < count; i++ {
		batch[i] = chanList[i]
		batchIDs[i] = channelIDs[i]
	}
	next := 8

	for i := 0; i < count; i++ {
		var result error
		var channelID string
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result = <-batch[0]:
			channelID = batchIDs[0]
			if next < count {
				batch[0] = chanList[next]
				batchIDs[0] = channelIDs[next]
			} else {
				batch[0] = nil
			}
		case result = <-batch[1]:
			channelID = batchIDs[1]
			if next < count {
				batch[1] = chanList[next]
				batchIDs[1] = channelIDs[next]
			} else {
				batch[1] = nil
			}
		case result = <-batch[2]:
			channelID = batchIDs[2]
			if next < count {
				batch[2] = chanList[next]
				batchIDs[2] = channelIDs[next]
			} else {
				batch[2] = nil
			}
		case result = <-batch[3]:
			channelID = batchIDs[3]
			if next < count {
				batch[3] = chanList[next]
				batchIDs[3] = channelIDs[next]
			} else {
				batch[3] = nil
			}
		case result = <-batch[4]:
			channelID = batchIDs[4]
			if next < count {
				batch[4] = chanList[next]
				batchIDs[4] = channelIDs[next]
			} else {
				batch[4] = nil
			}
		case result = <-batch[5]:
			channelID = batchIDs[5]
			if next < count {
				batch[5] = chanList[next]
				batchIDs[5] = channelIDs[next]
			} else {
				batch[5] = nil
			}
		case result = <-batch[6]:
			channelID = batchIDs[6]
			if next < count {
				batch[6] = chanList[next]
				batchIDs[6] = channelIDs[next]
			} else {
				batch[6] = nil
			}
		case result = <-batch[7]:
			channelID = batchIDs[7]
			if next < count {
				batch[7] = chanList[next]
				batchIDs[7] = channelIDs[next]
			} else {
				batch[7] = nil
			}
		}
		next++
		if reliable {
			err := call.Send(&pb.InternalPublishResponse{
				ChannelId: channelID,
				Success:   result == nil,
			})
			if err != nil {
				return err
			}
		} else if result != nil {
			log.Print("publish: ", result)
		}
	}

	return nil
}
