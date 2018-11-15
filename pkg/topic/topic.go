package topic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis"
	"github.com/gobuffalo/packr"
	"github.com/hpidcock/pub2sub/pkg/struuid"
)

var (
	ErrResultParseFailed   = errors.New("result parse failed")
	ErrFailedToCreateQueue = errors.New("failed to create queue")
	ErrQueueNotFound       = errors.New("queue not found")
	ErrSubscribeFailed     = errors.New("subscribe failed")
)

type Controller struct {
	redisClient         redis.UniversalClient
	addToTopic          *redis.Script
	createOrExtendQueue *redis.Script
	extendQueue         *redis.Script
	pushIfExists        *redis.Script
	topicGenerationAge  time.Duration
	clusterName         string
}

const (
	redisTopicKey   = "%s/t/%s/%d"
	redisChannelKey = "%s/c/%s"
)

func NewController(redisClient redis.UniversalClient, clusterName string) (*Controller, error) {
	var err error
	controller := Controller{
		redisClient:        redisClient,
		topicGenerationAge: 100 * time.Second,
		clusterName:        clusterName,
	}

	box := packr.NewBox("./lua")
	controller.addToTopic = redis.NewScript(box.String("add_to_topic.lua"))
	controller.createOrExtendQueue = redis.NewScript(box.String("create_or_extend_queue.lua"))
	controller.extendQueue = redis.NewScript(box.String("extend_queue.lua"))
	controller.pushIfExists = redis.NewScript(box.String("push_if_exists.lua"))

	if err != nil {
		return nil, err
	}

	return &controller, nil
}

func (m *Controller) GetTopicsWidth(ctx context.Context,
	topicIDs []struuid.UUID, asOf time.Time) ([]int64, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int(bucket / time.Second)

	cmds := make([]*redis.IntCmd, len(topicIDs))
	pipeline := m.redisClient.Pipeline()
	for k, topicID := range topicIDs {
		topicKey := fmt.Sprintf(redisTopicKey, m.clusterName, topicID.String(), suffix)
		cmds[k] = pipeline.ZCard(topicKey)
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

func (m *Controller) ScanTopic(ctx context.Context,
	topicID struuid.UUID, asOf time.Time,
	begin struuid.UUID, end struuid.UUID) ([]struuid.UUID, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int(bucket / time.Second)
	topicKey := fmt.Sprintf(redisTopicKey, m.clusterName, topicID.String(), suffix)

	pipeline := m.redisClient.Pipeline()
	res := pipeline.ZRangeByLex(topicKey, redis.ZRangeBy{
		Min: "(" + begin.String(),
		Max: "(" + end.String(),
	})
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}

	channelIDStrings := res.Val()
	channelIDs := make([]struuid.UUID, len(channelIDStrings))
	for k, v := range channelIDStrings {
		channelIDs[k] = struuid.Must(struuid.Parse(v))
	}

	return channelIDs, nil
}

func (m *Controller) CreateOrExtendQueue(ctx context.Context,
	channelID struuid.UUID, duration time.Duration) (bool, error) {
	seconds := int(math.Ceil(duration.Seconds()))

	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())

	res := m.createOrExtendQueue.Run(m.redisClient, []string{channelKey}, seconds)
	err := res.Err()
	if err != nil {
		return false, err
	}

	value, _ := res.Result()
	resInt, ok := value.(int64)
	if ok == false {
		return false, ErrResultParseFailed
	}

	if resInt != 1 && resInt != 2 {
		return false, ErrFailedToCreateQueue
	}

	// Return true if resumed.
	return resInt == 1, nil
}

func (m *Controller) DeleteQueue(ctx context.Context, channelID struuid.UUID) error {
	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())

	pipeline := m.redisClient.Pipeline()
	pipeline.Del(channelKey)
	_, err := pipeline.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (m *Controller) ExtendQueue(ctx context.Context,
	channelID struuid.UUID, duration time.Duration) error {
	seconds := int(math.Ceil(duration.Seconds()))
	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())

	res := m.extendQueue.Run(m.redisClient, []string{channelKey}, seconds)
	err := res.Err()
	if err != nil {
		return err
	}

	value, _ := res.Result()
	asInt, ok := value.(int64)
	if ok == false {
		return ErrResultParseFailed
	}

	if asInt != 1 {
		return ErrQueueNotFound
	}

	return nil
}

func (m *Controller) Subscribe(ctx context.Context,
	topicID struuid.UUID, asOf time.Time, channelID struuid.UUID) (time.Time, bool, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int64(bucket / time.Second)
	topicKey := fmt.Sprintf(redisTopicKey, m.clusterName, topicID.String(), suffix)
	expireAt := time.Unix(suffix, 0).Add(m.topicGenerationAge)

	res := m.addToTopic.Run(m.redisClient,
		[]string{topicKey},
		expireAt.Unix(),
		channelID.String())
	err := res.Err()
	if err != nil {
		return time.Time{}, false, err
	}

	value, _ := res.Result()
	asInt, ok := value.(int64)
	if ok == false {
		return time.Time{}, false, ErrResultParseFailed
	}

	exists := asInt == 0
	if asInt != 0 && asInt != 1 {
		return time.Time{}, false, ErrSubscribeFailed
	}

	return expireAt, exists, nil
}

func (m *Controller) Unsubscribe(ctx context.Context,
	topicID struuid.UUID, asOf time.Time, channelID struuid.UUID) (time.Time, bool, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int64(bucket / time.Second)
	topicKey := fmt.Sprintf(redisTopicKey, m.clusterName, topicID.String(), suffix)
	expireAt := time.Unix(suffix, 0).Add(m.topicGenerationAge)

	pipeline := m.redisClient.Pipeline()
	res := pipeline.ZRem(topicKey, channelID.String())
	_, err := pipeline.Exec()
	if err != nil {
		return time.Time{}, false, err
	}

	count, _ := res.Result()
	exists := count == 1

	return expireAt, exists, nil
}

func (m *Controller) PushMessage(ctx context.Context,
	channelID struuid.UUID, payload []byte) (string, error) {
	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())
	res := m.pushIfExists.Run(m.redisClient,
		[]string{channelKey},
		"p", payload)
	err := res.Err()
	if err == redis.Nil {
		return "", ErrQueueNotFound
	} else if err != nil {
		return "", err
	}

	value, _ := res.Result()
	str, ok := value.(string)
	if ok == false {
		return "", ErrResultParseFailed
	}

	return str, nil
}

func (m *Controller) DeleteMessage(ctx context.Context,
	channelID struuid.UUID, id string) error {
	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())
	err := m.redisClient.XDel(channelKey, id).Err()
	if err != nil {
		return err
	}

	return nil
}

type MessageEntry struct {
	ID      string
	Payload []byte
}

func (m *Controller) ReadMessages(ctx context.Context,
	channelID struuid.UUID, lastMessageID string, limit int) ([]MessageEntry, error) {
	channelKey := fmt.Sprintf(redisChannelKey, m.clusterName, channelID.String())
	if lastMessageID == "" {
		lastMessageID = "0-0"
	}

	// TODO: Batch reads on slotid
	res, err := m.redisClient.XRead(&redis.XReadArgs{
		Count:   int64(limit),
		Streams: []string{channelKey, lastMessageID},
		Block:   -1,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	entries := res[0].Messages

	var messages []MessageEntry
	for _, entry := range entries {
		value, _ := entry.Values["p"]
		messages = append(messages, MessageEntry{
			ID:      entry.ID,
			Payload: []byte(value.(string)),
		})
	}

	return messages, nil
}
