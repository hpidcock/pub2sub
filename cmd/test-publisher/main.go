package main

import (
	"context"
	"flag"
	"log"
	"time"

	"golang.org/x/sync/errgroup"

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

	eg, _ := errgroup.WithContext(context.Background())
	start := time.Now()
	failed := 0
	for x := 0; x < 8; x++ {
		c := x
		eg.Go(func() error {
			pubConnection, err := grpc.Dial("localhost:5001", grpc.WithInsecure())
			if err != nil {
				return err
			}
			pub := pb.NewPublishServiceClient(pubConnection)
			for i := c; i < sendCount; i += 8 {
				_, err = pub.Publish(context.Background(), &pb.PublishRequest{
					Id:       uuid.New().String(),
					Message:  []byte("hello"),
					Reliable: true,
					TopicIds: []string{topicID.String()},
					Ts:       time.Now().UnixNano(),
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		log.Fatal(err)
	}

	log.Print(time.Since(start))
	log.Printf("%d failed", failed)
}
