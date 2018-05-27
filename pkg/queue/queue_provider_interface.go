package queue

import (
	"context"
)

type QueueProviderInterface interface {
	CreateQueue(context.Context) (string, error)
}
