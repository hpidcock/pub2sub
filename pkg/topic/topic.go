package topic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis"
	"github.com/gobuffalo/packr"
	"github.com/google/uuid"
)

var (
	ErrResultParseFailed   = errors.New("result parse failed")
	ErrFailedToCreateQueue = errors.New("failed to create queue")
	ErrQueueNotFound       = errors.New("queue not found")
	ErrSubscribeFailed     = errors.New("subscribe failed")
)

type Controller struct {
	redisClient         redis.UniversalClient
	addToTopic          string
	createOrExtendQueue string
	extendQueue         string
	pushIfExists        string
	topicGenerationAge  time.Duration
}

func NewController(redisClient redis.UniversalClient) (*Controller, error) {
	var err error
	controller := Controller{
		redisClient:        redisClient,
		topicGenerationAge: 100 * time.Second,
	}

	box := packr.NewBox("./lua")
	controller.addToTopic, err = redisClient.ScriptLoad(
		box.String("add_to_topic.lua")).Result()
	controller.createOrExtendQueue, err = redisClient.ScriptLoad(
		box.String("create_or_extend_queue.lua")).Result()
	controller.extendQueue, err = redisClient.ScriptLoad(
		box.String("extend_queue.lua")).Result()
	controller.pushIfExists, err = redisClient.ScriptLoad(
		box.String("push_if_exists.lua")).Result()

	if err != nil {
		return nil, err
	}

	return &controller, nil
}

func (m *Controller) GetTopicsWidth(ctx context.Context,
	topicIDs []uuid.UUID, asOf time.Time) ([]int64, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int(bucket / time.Second)

	cmds := make([]*redis.IntCmd, len(topicIDs))
	pipeline := m.redisClient.Pipeline()
	for k, topicID := range topicIDs {
		topicKey := fmt.Sprintf("%s-%d", topicID.String(), suffix)
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
	topicID uuid.UUID, asOf time.Time,
	begin uuid.UUID, end uuid.UUID) ([]uuid.UUID, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int(bucket / time.Second)
	topicKey := fmt.Sprintf("%s-%d", topicID.String(), suffix)

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
	channelIDs := make([]uuid.UUID, len(channelIDStrings))
	for k, v := range channelIDStrings {
		channelIDs[k] = uuid.Must(uuid.Parse(v))
	}

	return channelIDs, nil
}

func (m *Controller) CreateOrExtendQueue(ctx context.Context,
	channelID uuid.UUID, duration time.Duration) (bool, error) {
	seconds := int(math.Ceil(duration.Seconds()))

	pipeline := m.redisClient.Pipeline()
	res := pipeline.EvalSha(m.createOrExtendQueue, []string{channelID.String()}, seconds)
	_, err := pipeline.Exec()
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

func (m *Controller) DeleteQueue(ctx context.Context, channelID uuid.UUID) error {
	pipeline := m.redisClient.Pipeline()
	pipeline.Del(channelID.String())
	_, err := pipeline.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (m *Controller) ExtendQueue(ctx context.Context,
	channelID uuid.UUID, duration time.Duration) error {
	seconds := int(math.Ceil(duration.Seconds()))

	pipeline := m.redisClient.Pipeline()
	res := pipeline.EvalSha(m.extendQueue, []string{channelID.String()}, seconds)
	_, err := pipeline.Exec()
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
	topicID uuid.UUID, asOf time.Time, channelID uuid.UUID) (time.Time, error) {
	bucket := time.Duration(asOf.Unix()) * time.Second
	bucket = bucket.Truncate(m.topicGenerationAge)
	suffix := int64(bucket / time.Second)
	topicKey := fmt.Sprintf("%s-%d", topicID.String(), suffix)
	expireAt := time.Unix(suffix, 0).Add(m.topicGenerationAge)

	pipeline := m.redisClient.Pipeline()
	res := pipeline.EvalSha(m.addToTopic,
		[]string{topicKey},
		expireAt.Unix(),
		channelID.String())
	_, err := pipeline.Exec()
	if err != nil {
		return time.Time{}, err
	}

	value, _ := res.Result()
	asInt, ok := value.(int64)
	if ok == false {
		return time.Time{}, ErrResultParseFailed
	}

	if asInt != 0 && asInt != 1 {
		return time.Time{}, ErrSubscribeFailed
	}

	return expireAt, nil
}

func (m *Controller) PushMessage(ctx context.Context,
	channelID uuid.UUID, payload []byte) (string, error) {
	pipeline := m.redisClient.Pipeline()
	res := pipeline.EvalSha(m.pushIfExists,
		[]string{channelID.String()},
		"p", payload)
	_, err := pipeline.Exec()
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
	channelID uuid.UUID, id string) error {
	err := m.redisClient.XDel(channelID.String(), id).Err()
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
	channelID uuid.UUID, lastMessageID string, limit int) ([]MessageEntry, error) {
	id := channelID.String()
	if lastMessageID == "" {
		lastMessageID = "0-0"
	}

	res, err := m.redisClient.XReadN(int64(limit),
		id, lastMessageID).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	entries, ok := res[id]
	if ok == false {
		return nil, nil
	}

	var messages []MessageEntry
	for _, entry := range entries {
		value, _ := entry.Fields["p"]
		messages = append(messages, MessageEntry{
			ID:      entry.Id,
			Payload: []byte(value),
		})
	}

	return messages, nil
}
