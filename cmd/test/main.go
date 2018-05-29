package main

import (
	"context"
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

	subConnection, err := grpc.Dial("localhost:5004", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	sub := pb.NewSubscribeServiceClient(subConnection)

	a := uuid.New()
	spew.Dump(a)

	log.Print("stream")
	stream, err := sub.Stream(context.Background(), &pb.StreamRequest{
		ChannelId: a.String(),
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	log.Print("lease")
	_, err = sub.Lease(context.Background(), &pb.LeaseRequest{
		ChannelId: a.String(),
		ExpireIn:  5,
		TopicId:   "abc",
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("publish")
	_, err = pub.Publish(context.Background(), &pb.PublishRequest{
		Id:       uuid.New().String(),
		Message:  []byte("hello"),
		Reliable: false,
		TopicIds: []string{"abc"},
		Ts:       time.Now().UnixNano(),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("stream recv")
	res, err := stream.Recv()
	if err != nil {
		log.Fatal(err)
	}

	spew.Dump(res)
}
