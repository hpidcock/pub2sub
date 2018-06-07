package main

import (
	"context"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func (p *Provider) HandleUDPChannel(data []byte) {
	var msg pb.UDPUnreliableMessage
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		log.Print(err)
		// TODO: Handle error better
		return
	}

	var outbound interface{}
	switch msg.Type {
	case pb.UDPMessageType_ACK:
		outbound = &pb.InternalAckMessage{
			ChannelId: msg.ChannelId,
			AckId:     msg.AckId,
		}
	case pb.UDPMessageType_EVICT:
		outbound = &pb.InternalEvictMessage{
			ChannelId: msg.ChannelId,
		}
	default:
		log.Print("unknown udp message type")
		return
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelFunc()
	err = p.router.Publish(ctx, msg.ChannelId, outbound)
	if err != nil {
		log.Print(err)
		// TODO: Handle error better
		return
	}
}
