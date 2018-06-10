package main

import (
	"context"
	"flag"
	"log"

	"github.com/google/uuid"

	"google.golang.org/grpc"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func main() {
	var topicIDString string
	var channelIDString string
	var sendCount int
	flag.StringVar(&channelIDString, "channel", "872b8833-0402-41ef-9096-53917fbf0286", "")
	flag.StringVar(&topicIDString, "topic", "e7619379-2ce4-426d-a9a7-31d530b6f59c", "")
	flag.IntVar(&sendCount, "count", 10, "")
	flag.Parse()

	channelID := uuid.Must(uuid.Parse(channelIDString))
	topicID := uuid.Must(uuid.Parse(topicIDString))

	subConnection, err := grpc.Dial("localhost:5005", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	sub := pb.NewSubscribeServiceClient(subConnection)

	log.Print("stream")
	stream, err := sub.Stream(context.Background(), &pb.StreamRequest{
		ChannelId: channelID.String(),
		Reliable:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	introMessage, err := stream.Recv()
	if err != nil {
		log.Fatal(err)
	}

	if evt, ok := introMessage.Event.(*pb.StreamResponse_StreamOpenedEvent); ok {
		log.Printf("resumed: %b\n", evt.StreamOpenedEvent.GetResumed())
	} else {
		log.Fatal("server returned the wrong event")
	}

	log.Print("lease")
	_, err = sub.Lease(context.Background(), &pb.LeaseRequest{
		ChannelId: channelID.String(),
		ExpireIn:  100,
		TopicId:   topicID.String(),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("stream recv")
	receiveCount := 0
	for {
		res, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}

		if evt, ok := res.Event.(*pb.StreamResponse_StreamMessageEvent); ok {
			if evt.StreamMessageEvent.Reliable == true {
				_, err = sub.Ack(context.Background(), &pb.AckRequest{
					AckId:     evt.StreamMessageEvent.AckId,
					ChannelId: channelID.String(),
					ServerId:  evt.StreamMessageEvent.ServerId,
				})
				if err != nil {
					log.Fatal(err)
				}
			}
		} else {
			log.Fatal("server returned the wrong event")
		}

		receiveCount++
		log.Print(receiveCount)
	}

	err = stream.CloseSend()
	if err != nil {
		log.Fatal(err)
	}
}
