package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/google/uuid"

	"google.golang.org/grpc"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func main() {
	var topicIDString string
	var sendCount int
	flag.StringVar(&topicIDString, "topic", "e7619379-2ce4-426d-a9a7-31d530b6f59c", "")
	flag.IntVar(&sendCount, "count", 10, "")
	flag.Parse()

	topicID := uuid.Must(uuid.Parse(topicIDString))

	pubConnection, err := grpc.Dial("localhost:5001", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	pub := pb.NewPublishServiceClient(pubConnection)

	start := time.Now()
	failed := 0
	for i := 0; i < sendCount; i++ {
		_, err = pub.Publish(context.Background(), &pb.PublishRequest{
			Id:       uuid.New().String(),
			Message:  []byte("hello"),
			Reliable: true,
			TopicIds: []string{topicID.String()},
			Ts:       time.Now().UnixNano(),
		})
		if err != nil {
			failed++
		}
	}

	log.Print(time.Since(start))
	log.Printf("%d failed", failed)
}
