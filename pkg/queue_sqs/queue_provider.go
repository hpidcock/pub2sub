package queue_sqs

import (
	"context"
	"errors"
)

type SQSQueueProvider struct {
}

func NewSQSQueueProvider() (*SQSQueueProvider, error) {
	return &SQSQueueProvider{}, nil
}

func (qp *SQSQueueProvider) CreateQueue(ctx context.Context) (string, error) {
	return "", errors.New("unimplemented")
}
