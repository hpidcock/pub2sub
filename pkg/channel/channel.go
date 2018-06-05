package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"
	multierror "github.com/hashicorp/go-multierror"
)

var (
	ErrChannelNotFound = errors.New("channel not found")
	ErrNoLease         = errors.New("no lease")
	ErrFailedAcquire   = errors.New("channel acquire failed")
	ErrFailedRelease   = errors.New("channel failed to release")
)

type RequestRelease func(ctx context.Context, serverID uuid.UUID, channelID uuid.UUID) error

type ChannelClient struct {
	etcdClient  *v3.Client
	leaseClient v3.Lease
	kvClient    v3.KV
	watchClient v3.Watcher

	leaseID     v3.LeaseID
	leaseIDLock sync.RWMutex

	serverID uuid.UUID
}

// NewChannelClient returns a new client for acquiring channels and their allocated server.
func NewChannelClient(etcdClient *v3.Client,
	serverID uuid.UUID) (*ChannelClient, error) {
	cc := &ChannelClient{
		etcdClient:  etcdClient,
		leaseClient: v3.NewLease(etcdClient),
		kvClient:    v3.NewKV(etcdClient),
		watchClient: v3.Watcher(etcdClient),
		serverID:    serverID,
	}

	return cc, nil
}

// GetChannelServerID returns the binded serverID or ErrChannelNotFound
// FIXME: Cache value in redis.
func (cc *ChannelClient) GetChannelServerID(ctx context.Context,
	channelID uuid.UUID) (uuid.UUID, error) {
	key := fmt.Sprintf("channel-%s", channelID.String())
	res, err := cc.kvClient.Get(ctx, key, v3.WithSerializable())
	if err != nil {
		return uuid.Nil, err
	}

	if len(res.Kvs) == 0 {
		return uuid.Nil, ErrChannelNotFound
	}

	serverID, err := uuid.Parse(string(res.Kvs[0].Value))
	if err != nil {
		return uuid.Nil, err
	}

	return serverID, nil
}

// AcquireChannel tries to acquire the channel, if it is already locked
// by annother service, request that server to release it.
func (cc *ChannelClient) AcquireChannel(ctx context.Context,
	channelID uuid.UUID, releaseFunc RequestRelease) error {
	cc.leaseIDLock.RLock()
	leaseID := cc.leaseID
	cc.leaseIDLock.RUnlock()
	if leaseID == 0 {
		return ErrNoLease
	}

	key := fmt.Sprintf("channel-%s", channelID.String())
	value := cc.serverID.String()

	prev, err := cc.kvClient.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(prev.Kvs) > 0 && prev.Kvs[0].CreateRevision != 0 {
		serverID, err := uuid.Parse(string(prev.Kvs[0].Value))
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

	return nil
}

// ReleaseChannel releases a channel so other servers can acquire it.
func (cc *ChannelClient) ReleaseChannel(ctx context.Context,
	channelID uuid.UUID) error {
	cc.leaseIDLock.RLock()
	leaseID := cc.leaseID
	cc.leaseIDLock.RUnlock()
	if leaseID == 0 {
		return ErrNoLease
	}

	key := fmt.Sprintf("channel-%s", channelID.String())

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
