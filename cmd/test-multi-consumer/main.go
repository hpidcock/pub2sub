package main

import (
	"context"
	"flag"
	"log"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"

	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

func main() {
	var topicIDString string
	var consumers int
	flag.StringVar(&topicIDString, "topic", "e7619379-2ce4-426d-a9a7-31d530b6f59c", "")
	flag.IntVar(&consumers, "consumers", 500, "")
	flag.Parse()

	topicID := uuid.Must(uuid.Parse(topicIDString))

	subConnection, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	sub := pb.NewSubscribeServiceClient(subConnection)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < consumers; i++ {
		eg.Go(func() error {
			return consumer(egCtx, sub, topicID)
		})
	}

	err = eg.Wait()
	if err != nil {
		log.Fatal(err)
	}
}

func consumer(ctx context.Context, sub pb.SubscribeServiceClient, topicID uuid.UUID) error {
	channelID := uuid.New()

	log.Print("stream")
	stream, err := sub.Stream(context.Background(), &pb.StreamRequest{
		ChannelId: channelID.String(),
		Reliable:  true,
	})
	if err != nil {
		return err
	}

	introMessage, err := stream.Recv()
	if err != nil {
		return err
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
		return err
	}

	log.Print("stream recv")
	receiveCount := 0
	for {
		res, err := stream.Recv()
		if err != nil {
			return err
		}

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

		receiveCount++
		log.Print(receiveCount)
	}

	err = stream.CloseSend()
	if err != nil {
		return err
	}

	return nil
}
