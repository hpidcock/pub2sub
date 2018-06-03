package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"

	"google.golang.org/grpc"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func main() {
	pubConnection, err := grpc.Dial("localhost:5001", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	pub := pb.NewPublishServiceClient(pubConnection)

	subConnection, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	sub := pb.NewSubscribeServiceClient(subConnection)

	channelID := uuid.Must(uuid.Parse("872b8833-0402-41ef-9096-53917fbf0286"))
	topicID := uuid.Must(uuid.Parse("e7619379-2ce4-426d-a9a7-31d530b6f59c"))
	spew.Dump(channelID)

	start := time.Now()

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

	start2 := time.Now()
	go func() {
		log.Print("publish")
		_, err = pub.Publish(context.Background(), &pb.PublishRequest{
			Id:       uuid.New().String(),
			Message:  []byte("hello"),
			Reliable: true,
			TopicIds: []string{topicID.String()},
			Ts:       time.Now().UnixNano(),
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Print("stream recv")
	res, err := stream.Recv()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start2))

	spew.Dump(res)

	if evt, ok := res.Event.(*pb.StreamResponse_StreamMessageEvent); ok {
		if evt.StreamMessageEvent.Reliable == true {
			_, err = sub.Ack(context.Background(), &pb.AckRequest{
				AckId:     evt.StreamMessageEvent.AckId,
				ChannelId: channelID.String(),
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		log.Fatal("server returned the wrong event")
	}

	fmt.Println(time.Since(start))
	fmt.Println(time.Since(start2))

	err = stream.CloseSend()
	if err != nil {
		log.Fatal(err)
	}
}
