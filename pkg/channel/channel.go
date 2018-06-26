package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hpidcock/pub2sub/pkg/struuid"
	"github.com/hpidcock/pub2sub/pkg/workerpool"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"
	multierror "github.com/hashicorp/go-multierror"
)

var (
	ErrChannelNotFound = errors.New("channel not found")
	ErrNoLease         = errors.New("no lease")
	ErrFailedAcquire   = errors.New("channel acquire failed")
	ErrFailedRelease   = errors.New("channel failed to release")
)

type RequestRelease func(ctx context.Context, serverID struuid.UUID, channelID struuid.UUID) error

type ChannelClient struct {
	etcdClient  *v3.Client
	leaseClient v3.Lease
	kvClient    v3.KV
	watchClient v3.Watcher

	leaseID     v3.LeaseID
	leaseIDLock sync.RWMutex

	serverID    struuid.UUID
	clusterName string

	redisClient    redis.UniversalClient
	etcdWorkerPool *workerpool.WorkerPool
}

const (
	channelCacheKey = "%s/cache/%s"
	channelEtcdKey  = "%s/channel/%s"
)

// NewChannelClient returns a new client for acquiring channels and their allocated server.
func NewChannelClient(etcdClient *v3.Client,
	redisClient redis.UniversalClient, clusterName string,
	serverID struuid.UUID) (*ChannelClient, error) {
	cc := &ChannelClient{
		etcdClient:     etcdClient,
		leaseClient:    v3.NewLease(etcdClient),
		kvClient:       v3.NewKV(etcdClient),
		watchClient:    v3.Watcher(etcdClient),
		serverID:       serverID,
		clusterName:    clusterName,
		redisClient:    redisClient,
		etcdWorkerPool: workerpool.New(etcdClient.Ctx()),
	}

	return cc, nil
}

func (cc *ChannelClient) BatchGetChannelServerID(ctx context.Context,
	channelIDs []struuid.UUID) (map[struuid.UUID]struuid.UUID, []struuid.UUID, error) {
	cmds := make(map[struuid.UUID]*redis.StringCmd)
	pipeline := cc.redisClient.Pipeline()
	for _, channelID := range channelIDs {
		channelIDString := channelID.String()
		cacheKey := fmt.Sprintf(channelCacheKey, cc.clusterName, channelIDString)
		cmds[channelID] = pipeline.Get(cacheKey)
	}
	pipeline.Exec()

	wg := sync.WaitGroup{}
	mut := sync.Mutex{}
	found := make(map[struuid.UUID]struuid.UUID)
	var notFound []struuid.UUID
	var errOut error
	var cachePipeline redis.Pipeliner

	etcdProcess := func(channelID struuid.UUID) error {
		channelIDString := channelID.String()
		key := fmt.Sprintf(channelEtcdKey, cc.clusterName, channelIDString)
		res, err := cc.kvClient.Get(ctx, key, v3.WithSerializable())
		if err != nil {
			return err
		}

		if len(res.Kvs) == 0 {
			mut.Lock()
			notFound = append(notFound, channelID)
			mut.Unlock()
			return nil
		}

		serverIDString := string(res.Kvs[0].Value)
		serverID, err := struuid.Parse(serverIDString)
		if err != nil {
			return err
		}

		mut.Lock()
		found[channelID] = serverID
		if cachePipeline == nil {
			cachePipeline = cc.redisClient.Pipeline()
		}

		cacheKey := fmt.Sprintf(channelCacheKey, cc.clusterName, channelIDString)
		cachePipeline.Set(cacheKey, serverIDString, 2*time.Minute)
		mut.Unlock()

		return nil
	}

	for v, redisResult := range cmds {
		channelID := v
		serverIDString, err := redisResult.Result()
		if err == redis.Nil {
			wg.Add(1)
			cc.etcdWorkerPool.Push(func() {
				defer wg.Done()
				err := etcdProcess(channelID)
				if err != nil {
					mut.Lock()
					errOut = multierror.Append(errOut, err)
					mut.Unlock()
				}
			})
			continue
		} else if err != nil {
			// TODO: handle error gracefully.
			return nil, nil, err
		}

		serverID, err := struuid.Parse(serverIDString)
		if err != nil {
			return nil, nil, err
		}

		mut.Lock()
		found[channelID] = serverID
		mut.Unlock()
	}

	wg.Wait()

	if cachePipeline != nil {
		cachePipeline.Exec()
	}

	return found, notFound, nil
}

// GetChannelServerID returns the binded serverID or ErrChannelNotFound
func (cc *ChannelClient) GetChannelServerID(ctx context.Context,
	channelID struuid.UUID) (struuid.UUID, error) {
	channelIDString := channelID.String()
	cacheKey := fmt.Sprintf(channelCacheKey, cc.clusterName, channelIDString)
	cachedServerIDString, err := cc.redisClient.Get(cacheKey).Result()
	if err == redis.Nil {
	} else if err != nil {
		return struuid.Nil, err
	} else {
		serverID, err := struuid.Parse(cachedServerIDString)
		if err != nil {
			// TODO: handle error gracefully.
			return struuid.Nil, err
		}
		return serverID, nil
	}

	key := fmt.Sprintf(channelEtcdKey, cc.clusterName, channelIDString)
	res, err := cc.kvClient.Get(ctx, key, v3.WithSerializable())
	if err != nil {
		return struuid.Nil, err
	}

	if len(res.Kvs) == 0 {
		return struuid.Nil, ErrChannelNotFound
	}

	serverIDString := string(res.Kvs[0].Value)
	serverID, err := struuid.Parse(serverIDString)
	if err != nil {
		return struuid.Nil, err
	}

	// TODO: configure expiry.
	err = cc.redisClient.Set(cacheKey, serverIDString, 2*time.Minute).Err()
	if err != nil {
		// TODO: handle error gracefully.
		return struuid.Nil, err
	}

	return serverID, nil
}

// AcquireChannel tries to acquire the channel, if it is already locked
// by annother service, request that server to release it.
func (cc *ChannelClient) AcquireChannel(ctx context.Context,
	channelID struuid.UUID, releaseFunc RequestRelease) error {
	cc.leaseIDLock.RLock()
	leaseID := cc.leaseID
	cc.leaseIDLock.RUnlock()
	if leaseID == 0 {
		return ErrNoLease
	}

	key := fmt.Sprintf(channelEtcdKey, cc.clusterName, channelID.String())
	value := cc.serverID.String()

	prev, err := cc.kvClient.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(prev.Kvs) > 0 && prev.Kvs[0].CreateRevision != 0 {
		serverID, err := struuid.Parse(string(prev.Kvs[0].Value))
		if err != nil {
			return err
		}

		// If the other server releases the channel, it should be deleted.
		err = releaseFunc(ctx, serverID, channelID)
		if err != nil {
			return err
		}
	}

	txn := cc.kvClient.Txn(ctx)
	txn = txn.If(v3.Compare(v3.CreateRevision(key), "=", 0))
	txn = txn.Then(v3.OpPut(key, value, v3.WithLease(leaseID)))
	res, err := txn.Commit()
	if err != nil {
		return err
	}

	if res.Succeeded == false {
		return ErrFailedAcquire
	}

	cacheKey := fmt.Sprintf(channelCacheKey, cc.clusterName, channelID.String())
	err = cc.redisClient.Del(cacheKey).Err()
	if err != nil {
		return err
	}

	return nil
}

// ReleaseChannel releases a channel so other servers can acquire it.
func (cc *ChannelClient) ReleaseChannel(ctx context.Context,
	channelID struuid.UUID) error {
	cc.leaseIDLock.RLock()
	leaseID := cc.leaseID
	cc.leaseIDLock.RUnlock()
	if leaseID == 0 {
		return ErrNoLease
	}

	key := fmt.Sprintf(channelEtcdKey, cc.clusterName, channelID.String())

	txn := cc.kvClient.Txn(ctx)
	txn = txn.If(v3.Compare(v3.LeaseValue(key), "=", leaseID))
	txn = txn.Then(v3.OpDelete(key))
	res, err := txn.Commit()
	if err != nil {
		return err
	}

	if res.Succeeded == false {
		return ErrFailedRelease
	}

	return nil
}

// Lease maintains a lease with etcd
func (cc *ChannelClient) Lease(ctx context.Context) (errOut error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	grant, err := cc.leaseClient.Grant(ctx, 30)
	if err != nil {
		return err
	}

	leaseExpire := time.Now().Add(time.Duration(grant.TTL) * time.Second)

	cc.leaseIDLock.Lock()
	cc.leaseID = grant.ID
	cc.leaseIDLock.Unlock()

	defer func() {
		cc.leaseIDLock.Lock()
		cc.leaseID = 0
		cc.leaseIDLock.Unlock()
		revokeCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc()
		_, err := cc.leaseClient.Revoke(revokeCtx, grant.ID)
		if err != nil {
			errOut = multierror.Append(errOut, err)
			return
		}
	}()

	keepAliveChannel, err := cc.leaseClient.KeepAlive(ctx, grant.ID)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
