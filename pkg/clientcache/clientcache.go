package clientcache

import (
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/koding/cache"
	"google.golang.org/grpc"
)

type ClientCache struct {
	c      *cache.MemoryTTL
	m      sync.Mutex
	closed bool
}

func NewCache(ttl time.Duration, gcInterval time.Duration) *ClientCache {
	cc := &ClientCache{
		c: cache.NewMemoryWithTTL(ttl),
	}
	cc.c.StartGC(gcInterval)
	return cc
}

func (cc *ClientCache) Close() {
	cc.m.Lock()
	defer cc.m.Unlock()
	cc.c.StopGC()
	cc.closed = true
}

func (cc *ClientCache) Connect(raddr string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	cacheConn, err := cc.c.Get(raddr)
	if err == cache.ErrNotFound {
	} else if err != nil {
		return nil, err
	} else {
		conn = cacheConn.(*grpc.ClientConn)
		state := conn.GetState()
		switch state {
		case connectivity.Idle:
			return conn, nil
		case connectivity.Connecting:
			return conn, nil
		case connectivity.Ready:
			return conn, nil
		}
	}

	cc.m.Lock()
	defer cc.m.Unlock()

	cacheConn, err = cc.c.Get(raddr)
	if err == nil {
		newConn := cacheConn.(*grpc.ClientConn)
		if newConn != conn {
			// We raced to create a new connection
			// annother thread won.
			return newConn, nil
		}
	}

	conn, err = grpc.Dial(raddr, grpc.WithInsecure())
	if err != nil {
		cc.c.Delete(raddr)
		return nil, err
	}

	cc.c.Set(raddr, conn)
	return conn, nil
}
