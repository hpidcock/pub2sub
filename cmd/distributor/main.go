package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	etcd_clientv3 "github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/hpidcock/pub2sub/pkg/channel"
	"github.com/hpidcock/pub2sub/pkg/clientcache"
	"github.com/hpidcock/pub2sub/pkg/discovery"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
	"github.com/hpidcock/pub2sub/pkg/struuid"
	"github.com/hpidcock/pub2sub/pkg/topic"
	"github.com/hpidcock/pub2sub/pkg/workerpool"
)

type Provider struct {
	config   Config
	serverID struuid.UUID

	redisClient redis.UniversalClient
	etcdClient  *etcd_clientv3.Client
	grpcClients *clientcache.ClientCache

	topicController *topic.Controller
	channelClient   *channel.ChannelClient
	disc            *discovery.DiscoveryClient
	planners        *discovery.DiscoveryList

	plannerDispatchWorkerPool *workerpool.WorkerPool
}

func (p *Provider) runGRPCServer(ctx context.Context) error {
	endpoint := fmt.Sprintf(":%d", p.config.Port)
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return err
	}

	server := grpc.NewServer(
		grpc.WriteBufferSize(0),
	)
	pb.RegisterDistributeServiceServer(server, p)

	closeChan := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
		case <-closeChan:
			return
		}
	}()

	log.Printf("serving gRPC endpoints on %s", endpoint)
	err = server.Serve(listener)
	defer close(closeChan)
	if err != nil {
		return err
	}

	return nil
}

func (p *Provider) runDiscoveryBroadcast(ctx context.Context) error {
	serviceName := "distributors"
	endpoint := fmt.Sprintf("%s:%d", p.config.AnnouceAddress, p.config.Port)
	log.Printf("etcd: broadcasting %s %s at %s", serviceName, p.serverID, endpoint)
	return p.disc.Broadcast(ctx, serviceName, p.serverID, endpoint)
}

func (p *Provider) runLayerDiscovery(ctx context.Context) error {
	log.Printf("etcd: discovering planners")
	return p.planners.Watch(ctx, "planners")
}

func (p *Provider) init() error {
	var err error
	if p.config.RedisCluster {
		p.redisClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{p.config.RedisAddress},
		})
	} else {
		p.redisClient = redis.NewClient(&redis.Options{
			Addr: p.config.RedisAddress,
		})
	}

	p.grpcClients = clientcache.NewCache(60*time.Second, 10*time.Second)

	p.topicController, err = topic.NewController(p.redisClient, p.config.ClusterName)
	if err != nil {
		return err
	}

	etcdConfig := etcd_clientv3.Config{
		Endpoints: []string{
			p.config.EtcdAddress,
		},
	}
	p.etcdClient, err = etcd_clientv3.New(etcdConfig)
	if err != nil {
		return err
	}

	p.disc, err = discovery.NewDiscoveryClient(p.etcdClient, p.config.ClusterName)
	if err != nil {
		return err
	}

	p.planners, err = discovery.NewDiscoveryList(p.disc)
	if err != nil {
		return err
	}

	p.channelClient, err = channel.NewChannelClient(p.etcdClient, p.redisClient,
		p.config.ClusterName, p.serverID)
	if err != nil {
		return err
	}

	return nil
}

func (p *Provider) close() error {
	err := p.redisClient.Close()
	if err != nil {
		return err
	}

	err = p.etcdClient.Close()
	if err != nil {
		return err
	}

	p.grpcClients.Close()

	return nil
}

func run(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	var err error
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Print("starting pub2sub distributor")

	provider := &Provider{
		serverID:                  struuid.FromUUID(uuid.New()),
		plannerDispatchWorkerPool: workerpool.New(ctx),
	}
	provider.config, err = NewConfig()
	if err != nil {
		return err
	}

	err = provider.init()
	if err != nil {
		return err
	}
	defer provider.close()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return provider.runGRPCServer(egCtx)
	})
	eg.Go(func() error {
		return provider.runDiscoveryBroadcast(egCtx)
	})
	eg.Go(func() error {
		return provider.runLayerDiscovery(egCtx)
	})
	err = eg.Wait()
	if err != nil {
		return err
	}

	return nil
}

func main() {
	var err error
	ctx, cancelFunc := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	defer func() {
		signal.Stop(sigChan)
		close(sigChan)
	}()
	go func() {
		s, ok := <-sigChan
		if ok {
			log.Printf("received signal %s", s.String())
		}
		cancelFunc()
	}()

	err = run(ctx)
	time.Sleep(1 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
}
