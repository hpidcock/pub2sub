package queue

import (
	"context"
)

type QueueProviderInterface interface {
	CreateQueue(ctx context.Context) (string, error)
	PublishQueue(ctx context.Context, queueID string, message []byte) error
}
