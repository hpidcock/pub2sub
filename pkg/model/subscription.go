package model

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/hashicorp/go-multierror"

	"github.com/hpidcock/pub2sub/pkg/queue"
)

var (
	ErrNoChannel = errors.New("no channel")
)

type ChannelModel struct {
	ChannelID string
	QueueID   string
	ServerID  string
	ExpireAt  time.Time

	// Only acquire sees this
	Clock int64
}

type Controller struct {
	queueProvider   queue.QueueProviderInterface
	redisClient     redis.UniversalClient
	hashSetExpireGT string
}

func NewController(queueProvider queue.QueueProviderInterface,
	redisClient redis.UniversalClient) (*Controller, error) {
	var err error
	controller := Controller{
		queueProvider: queueProvider,
		redisClient:   redisClient,
	}

	controller.hashSetExpireGT, err = redisClient.ScriptLoad(`
		local c = tonumber(redis.call('hget', KEYS[1], 'expire_at'));
		if c == nil then
			return nil
		end
		local n = tonumber(ARGV[1])
		if n < c then
			return c
		end
		redis.call('hset', KEYS[1], 'expire_at', ARGV[1])
		return n
	`).Result()

	if err != nil {
		return nil, err
	}

	return &controller, nil
}

func (m *Controller) AcquireQueue(ctx context.Context) (string, error) {
	queueID, err := m.redisClient.LPop("queues").Result()
	if err == redis.Nil {
		queueID, err = m.queueProvider.CreateQueue(ctx)
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}

	return queueID, nil
}

func (m *Controller) ReleaseQueue(ctx context.Context, queueID string) error {
	_, err := m.redisClient.LPush("queues", queueID).Result()
	if err != nil {
		log.Printf("queue %s failed to be released due to %v", queueID, err)
		return err
	}

	return nil
}

func (m *Controller) GetTopicsWidth(ctx context.Context, topicIDs []string) ([]int64, error) {
	cmds := make([]*redis.IntCmd, len(topicIDs))
	pipeline := m.redisClient.Pipeline()
	for k, topicID := range topicIDs {
		cmds[k] = pipeline.ZCard(topicID)
	}
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}

	results := make([]int64, len(topicIDs))
	for k, v := range cmds {
		results[k] = v.Val()
	}

	return results, nil
}

func (m *Controller) ScanTopic(ctx context.Context, topicID string,
	begin string, end string) ([]string, error) {
	pipeline := m.redisClient.Pipeline()
	res := pipeline.ZRangeByLex(topicID, redis.ZRangeBy{
		Min: begin,
		Max: end,
	})
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}

	return res.Val(), nil
}

func (m *Controller) ScanExpiredSubscriptions(ctx context.Context, topicID string,
	before time.Time, limit int) ([]string, error) {
	pipeline := m.redisClient.Pipeline()
	res := pipeline.ZRangeByScore("x_"+topicID, redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(before.UTC().Unix(), 10),
		Count: int64(limit),
	})
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}

	return res.Val(), nil
}

func (m *Controller) Subscribe(ctx context.Context,
	channelID string, topicID string, expireAt time.Time) error {
	_, err := m.ChannelExpireAfter(ctx, channelID, expireAt)
	if err != nil {
		return err
	}

	pipeline := m.redisClient.Pipeline()
	pipeline.ZAdd("x_"+topicID, redis.Z{
		Member: channelID,
		Score:  float64(expireAt.UTC().Unix()),
	})
	pipeline.ZAdd(topicID, redis.Z{
		Member: channelID,
	})
	_, err = pipeline.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (m *Controller) DeleteChannel(ctx context.Context,
	channelID string) error {
	return nil
}

func (m *Controller) AcquireChannel(ctx context.Context,
	channelID string, serverID string, reliable bool) (*ChannelModel, *ChannelModel, error) {
	var err error
	oldChannel := ChannelModel{
		ChannelID: channelID,
		Clock:     -1,
	}

	newChannel := ChannelModel{
		ChannelID: channelID,
		ServerID:  serverID,
	}

	err = m.redisClient.Watch(func(tx *redis.Tx) (errOut error) {
		oldChannel.ServerID, err = tx.HGet(channelID, "server_id").Result()
		if err == redis.Nil {
		} else if err != nil {
			return err
		}

		oldChannel.QueueID, err = tx.HGet(channelID, "queue_id").Result()
		if err == redis.Nil {
		} else if err != nil {
			return err
		}

		if reliable && oldChannel.QueueID == "" {
			newChannel.QueueID, err = m.AcquireQueue(ctx)
			if err != nil {
				return err
			}
			defer func() {
				if errOut != nil {
					err := m.ReleaseQueue(ctx, newChannel.QueueID)
					if err != nil {
						errOut = multierror.Append(errOut, err)
					}
				}
			}()
		} else if reliable {
			newChannel.QueueID = oldChannel.QueueID
		}

		var clockIncrCommand *redis.IntCmd
		_, err = tx.Pipelined(func(pipeline redis.Pipeliner) error {
			clockIncrCommand = pipeline.HIncrBy(channelID, "clock", 1)
			pipeline.HSet(channelID, "server_id", newChannel.ServerID)
			if newChannel.QueueID == "" {
				pipeline.HDel(channelID, "queue_id")
			} else if newChannel.QueueID != oldChannel.QueueID {
				pipeline.HSet(channelID, "queue_id", newChannel.QueueID)
			}
			return nil
		})

		if err != nil {
			return err
		}

		newChannel.Clock = clockIncrCommand.Val()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	if reliable == false &&
		oldChannel.QueueID != "" &&
		newChannel.QueueID == "" {
		err = m.ReleaseQueue(ctx, oldChannel.QueueID)
		if err != nil {
			return nil, nil, err
		}
	}

	return &oldChannel, &newChannel, nil
}

func (m *Controller) ChannelExpireAfter(ctx context.Context,
	channelID string, expireAfter time.Time) (time.Time, error) {
	ts := expireAfter.UTC().Unix()

	pipeline := m.redisClient.Pipeline()
	result := pipeline.EvalSha(m.hashSetExpireGT, []string{channelID}, ts)
	_, err := pipeline.Exec()
	if err == redis.Nil {
		return time.Time{}, ErrNoChannel
	} else if err != nil {
		return time.Time{}, err
	}

	currentTs := result.Val().(int64)
	return time.Unix(currentTs, 0), nil
}

func (m *Controller) GetChannel(ctx context.Context,
	channelID string) (*ChannelModel, error) {
	channel := ChannelModel{
		ChannelID: channelID,
		Clock:     -1,
	}

	pipeline := m.redisClient.Pipeline()
	serverID := pipeline.HGet(channelID, "server_id")
	queueID := pipeline.HGet(channelID, "queue_id")
	expireAt := pipeline.HGet(channelID, "expire_at")
	_, err := pipeline.Exec()
	if err == redis.Nil {
		return nil, ErrNoChannel
	} else if err != nil {
		return nil, err
	}

	channel.ServerID = serverID.Val()
	channel.QueueID = queueID.Val()
	expireAtTs, err := expireAt.Int64()
	if err != nil {
		return nil, err
	}
	channel.ExpireAt = time.Unix(expireAtTs, 0)

	return &channel, nil
}

func (m *Controller) GetChannelClock(ctx context.Context,
	channelID string) (int64, error) {

	pipeline := m.redisClient.Pipeline()
	clock := pipeline.HGet(channelID, "clock")
	_, err := pipeline.Exec()
	if err == redis.Nil {
		return -1, ErrNoChannel
	} else if err != nil {
		return -1, err
	}

	value, err := clock.Int64()
	if err != nil {
		return -1, err
	}

	return value, nil
}
