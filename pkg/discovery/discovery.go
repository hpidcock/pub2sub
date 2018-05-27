package discovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	etcd_clientv3 "github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	funk "github.com/thoas/go-funk"
)

type DiscoveryClient struct {
	etcdClient  *etcd_clientv3.Client
	leaseClient etcd_clientv3.Lease
	kvClient    etcd_clientv3.KV
	watchClient etcd_clientv3.Watcher
}

func NewDiscoveryClient(etcdClient *etcd_clientv3.Client) (*DiscoveryClient, error) {
	dc := &DiscoveryClient{
		etcdClient:  etcdClient,
		leaseClient: etcd_clientv3.NewLease(etcdClient),
		kvClient:    etcd_clientv3.NewKV(etcdClient),
		watchClient: etcd_clientv3.Watcher(etcdClient),
	}

	return dc, nil
}

func (dc *DiscoveryClient) Broadcast(ctx context.Context, serviceName string,
	serverID uuid.UUID, value string) (errOut error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	grant, err := dc.leaseClient.Grant(ctx, 30)
	if err != nil {
		return err
	}
	leaseID := grant.ID
	leaseExpire := time.Now().Add(time.Duration(grant.TTL) * time.Second)
	defer func() {
		revokeCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc()
		_, err := dc.leaseClient.Revoke(revokeCtx, leaseID)
		if err != nil {
			errOut = multierror.Append(errOut, err)
			return
		}
	}()

	keyName := fmt.Sprintf("%s-%s", serviceName, serverID.String())
	_, err = dc.kvClient.Put(ctx, keyName, value, etcd_clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}

	keepAliveChannel, err := dc.leaseClient.KeepAlive(ctx, leaseID)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case keepAlive, ok := <-keepAliveChannel:
			if ok == false {
				return context.Canceled
			}
			leaseExpire = time.Now().Add(time.Duration(keepAlive.TTL) * time.Second)
		case <-time.After(time.Until(leaseExpire)):
			return context.Canceled
		}
	}
}

type DiscoveryList struct {
	discoveryClient *DiscoveryClient
	list            atomic.Value
	watchLock       sync.Mutex
}

func NewDiscoveryList(discoveryClient *DiscoveryClient) (*DiscoveryList, error) {
	dl := &DiscoveryList{
		discoveryClient: discoveryClient,
	}

	return dl, nil
}

func (dl *DiscoveryList) GetList() map[uuid.UUID]string {
	return dl.list.Load().(map[uuid.UUID]string)
}

// Watch should only be called once.
func (dl *DiscoveryList) Watch(ctx context.Context, serviceName string) (errOut error) {
	dl.watchLock.Lock()
	defer dl.watchLock.Unlock()

	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	servicePrefix := fmt.Sprintf("%s-", serviceName)
	beginKey := fmt.Sprintf("%s00000000-0000-0000-0000-000000000000", servicePrefix)
	endKey := fmt.Sprintf("%sffffffff-ffff-ffff-ffff-ffffffffffff", servicePrefix)

	watchChan := dl.discoveryClient.watchClient.Watch(ctx, beginKey, etcd_clientv3.WithRange(endKey))
	res, err := dl.discoveryClient.kvClient.Get(ctx, beginKey, etcd_clientv3.WithRange(endKey))
	if err != nil {
		return err
	}

	current := make(map[string]string)
	for _, kv := range res.Kvs {
		current[string(kv.Key)] = string(kv.Value)
	}

	update := func() {
		dl.list.Store(funk.Map(current, func(key string, value string) (uuid.UUID, string) {
			id := strings.TrimPrefix(key, servicePrefix)
			return uuid.Must(uuid.Parse(id)), value
		}))
	}
	update()

	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case watch, ok := <-watchChan:
			if ok == false || watch.Canceled {
				return context.Canceled
			}

			for _, event := range watch.Events {
				switch event.Type {
				case etcd_clientv3.EventTypeDelete:
					delete(current, string(event.Kv.Key))
				case etcd_clientv3.EventTypePut:
					current[string(event.Kv.Key)] = string(event.Kv.Value)
				}
			}

			update()
		}
	}
}
