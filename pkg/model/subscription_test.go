package model

import (
	"context"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

type MockQueueProvider struct {
	t *testing.T
}

func (qp *MockQueueProvider) CreateQueue(ctx context.Context) (string, error) {
	return "", nil
}

func TestSubscribe(t *testing.T) {
	var err error
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	err = client.FlushAll().Err()
	assert.NoError(t, err)

	queueController := &MockQueueProvider{
		t: t,
	}

	ctx := context.Background()

	controller := NewController(queueController, client)
	sm0, err := controller.AcquireSubscription(ctx, "i0", "topic", "subscriber", "topic-subscriber", "s0")
	assert.NoError(t, err)

	sm1, err := controller.GetSubscription(ctx, "topic-subscriber")
	assert.NoError(t, err)
	assert.Equal(t, sm0.CreatedAt, sm1.CreatedAt)
	assert.Equal(t, sm0.InstanceID, "i0")
	assert.Equal(t, sm0.QueueID, sm1.QueueID)
	assert.Equal(t, sm0.ServerID, "s0")
	assert.Equal(t, sm0.SubscriberID, sm1.SubscriberID)

	sm2, err := controller.AcquireSubscription(ctx, "i1", "topic", "subscriber", "topic-subscriber", "s1")
	assert.NoError(t, err)
	assert.Equal(t, sm2.CreatedAt, sm1.CreatedAt)
	assert.Equal(t, sm2.InstanceID, "i1")
	assert.Equal(t, sm2.QueueID, sm1.QueueID)
	assert.Equal(t, sm2.ServerID, "s1")
	assert.Equal(t, sm2.SubscriberID, sm1.SubscriberID)
}
