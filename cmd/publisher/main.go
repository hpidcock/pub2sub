package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	etcd_clientv3 "github.com/coreos/etcd/clientv3"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/lucas-clemente/quic-go/h2quic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/hpidcock/pub2sub/pkg/discovery"
	"github.com/hpidcock/pub2sub/pkg/model"
	pb "github.com/hpidcock/pub2sub/pkg/pub2subpb"
)

type Provider struct {
	config   Config
	serverID uuid.UUID

	redisClient *redis.Client
	etcdClient  *etcd_clientv3.Client

	modelController *model.Controller
	disc            *discovery.DiscoveryClient
	replicators     *discovery.DiscoveryList
}

func (p *Provider) runGRPCServer(ctx context.Context) error {
	endpoint := fmt.Sprintf(":%d", p.config.Port)
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	pb.RegisterPublishServiceServer(server, p)

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

func (p *Provider) runQUICServer(ctx context.Context) error {
	endpoint := fmt.Sprintf(":%d", p.config.Port)

	handler := pb.NewPublishServiceServer(p, nil)
	server := h2quic.Server{
		Server: &http.Server{
			Addr:    endpoint,
			Handler: handler,
		},
	}

	closeChan := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.Close()
		case <-closeChan:
			return
		}
	}()

	log.Printf("serving twirp/QUIC endpoints on %s", endpoint)
	err := server.ListenAndServeTLS("server.crt", "server.key")
	defer close(closeChan)
	if err != nil {
		return err
	}

	return nil
}

func (p *Provider) runDiscoveryBroadcast(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s:%d", hostname, p.config.Port)
	log.Printf("etcd: broadcasting publisher %s at %s", p.serverID, endpoint)
	return p.disc.Broadcast(ctx, "publisher", p.serverID, endpoint)
}

func (p *Provider) runReplicatorDiscovery(ctx context.Context) error {
	log.Printf("etcd: discovering replicators on layer 0")
	return p.replicators.Watch(ctx, "replicators-0")
}

func (p *Provider) init() error {
	var err error
	p.redisClient = redis.NewClient(&redis.Options{
		Addr: p.config.RedisAddress,
	})

	p.modelController, err = model.NewController(nil, p.redisClient)
	if err != nil {
		return err
	}

	etcdConfig := etcd_clientv3.Config{
		Endpoints: []string{
			// TODO: Config
			"localhost:2379",
		},
	}
	p.etcdClient, err = etcd_clientv3.New(etcdConfig)
	if err != nil {
		return err
	}

	p.disc, err = discovery.NewDiscoveryClient(p.etcdClient)
	if err != nil {
		return err
	}

	p.replicators, err = discovery.NewDiscoveryList(p.disc)
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

	return nil
}

func run(ctx context.Context) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	var err error
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Print("starting pub2sub publisher")

	provider := &Provider{
		serverID: uuid.New(),
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

	go func() {
		for {
			time.Sleep(1 * time.Second)
			spew.Dump(provider.replicators.GetList())
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return provider.runGRPCServer(egCtx)
	})
	eg.Go(func() error {
		return provider.runQUICServer(egCtx)
	})
	eg.Go(func() error {
		return provider.runDiscoveryBroadcast(egCtx)
	})
	eg.Go(func() error {
		return provider.runReplicatorDiscovery(egCtx)
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
