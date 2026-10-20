package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskQueue interface {
	Push(ctx context.Context, message Message) error
	PushBatch(ctx context.Context, messages []Message) error
	Pop(ctx context.Context) (Delivery, error)
	Ack(ctx context.Context, deliveryID string) error
}

type Message struct {
	TaskID     uint64
	Generation uint64
}

type Stats struct {
	StreamLength int64
	Pending      int64
	Lag          int64
}

type StatsProvider interface {
	Stats(ctx context.Context) (Stats, error)
}

type Delivery struct {
	ID          string
	TaskID      uint64
	Generation  uint64
	Backend     string
	Source      string
	RawEnvelope string
}

type PoisonDeliveryError struct {
	Backend      string
	Source       string
	DeliveryID   string
	RawEnvelope  string
	FailureClass string
	Reason       string
	TaskID       *uint64
	Generation   *uint64
}

func (e *PoisonDeliveryError) Error() string { return e.Reason }

type RedisTaskQueue struct {
	client     *redis.Client
	key        string
	group      string
	consumer   string
	claimIdle  time.Duration
	groupMu    sync.Mutex
	groupReady bool
}

func NewRedisTaskQueue(client *redis.Client, key string) *RedisTaskQueue {
	host, _ := os.Hostname()
	return &RedisTaskQueue{
		client:    client,
		key:       key,
		group:     "taskflow-workers",
		consumer:  fmt.Sprintf("%s-%d", host, os.Getpid()),
		claimIdle: 30 * time.Second,
	}
}

func (q *RedisTaskQueue) ensureGroup(ctx context.Context) error {
	q.groupMu.Lock()
	defer q.groupMu.Unlock()
	if q.groupReady {
		return nil
	}

	err := q.client.XGroupCreateMkStream(ctx, q.key, q.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	q.groupReady = true
	return nil
}

func (q *RedisTaskQueue) invalidateGroup() {
	q.groupMu.Lock()
	q.groupReady = false
	q.groupMu.Unlock()
}

func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

func (q *RedisTaskQueue) Push(ctx context.Context, message Message) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.key,
		Values: map[string]any{
			"task_id":            strconv.FormatUint(message.TaskID, 10),
			"enqueue_generation": strconv.FormatUint(message.Generation, 10),
		},
	}).Err()
}

func (q *RedisTaskQueue) PushBatch(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	_, err := q.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, message := range messages {
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: q.key,
				Values: map[string]any{
					"task_id":            strconv.FormatUint(message.TaskID, 10),
					"enqueue_generation": strconv.FormatUint(message.Generation, 10),
				},
			})
		}
		return nil
	})
	return err
}

func (q *RedisTaskQueue) Pop(ctx context.Context) (Delivery, error) {
	if err := q.ensureGroup(ctx); err != nil {
		return Delivery{}, err
	}

	// Reclaim a message whose previous consumer likely died before ACK.
	messages, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: q.key, Group: q.group, Consumer: q.consumer,
		MinIdle: q.claimIdle, Start: "0-0", Count: 1,
	}).Result()
	if isNoGroup(err) {
		q.invalidateGroup()
		if groupErr := q.ensureGroup(ctx); groupErr != nil {
			return Delivery{}, groupErr
		}
		err = nil
	}
	if err == nil && len(messages) > 0 {
		return q.parseRedisDelivery(messages[0])
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return Delivery{}, err
	}

	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: q.group, Consumer: q.consumer,
		Streams: []string{q.key, ">"}, Count: 1, Block: 2 * time.Second,
	}).Result()
	if isNoGroup(err) {
		q.invalidateGroup()
		if groupErr := q.ensureGroup(ctx); groupErr != nil {
			return Delivery{}, groupErr
		}
		return Delivery{}, redis.Nil
	}
	if err != nil {
		return Delivery{}, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Delivery{}, redis.Nil
	}
	return q.parseRedisDelivery(streams[0].Messages[0])
}

func (q *RedisTaskQueue) parseRedisDelivery(msg redis.XMessage) (Delivery, error) {
	rawJSON, _ := json.Marshal(msg.Values)
	rawEnvelope := string(rawJSON)
	raw, ok := msg.Values["task_id"]
	if !ok {
		return Delivery{}, &PoisonDeliveryError{
			Backend: "redis", Source: q.key, DeliveryID: msg.ID,
			RawEnvelope: rawEnvelope, FailureClass: "decode", Reason: "stream message missing task_id",
		}
	}
	id, err := strconv.ParseUint(fmt.Sprint(raw), 10, 64)
	if err != nil {
		return Delivery{}, &PoisonDeliveryError{
			Backend: "redis", Source: q.key, DeliveryID: msg.ID,
			RawEnvelope: rawEnvelope, FailureClass: "decode", Reason: fmt.Sprintf("parse task_id: %v", err),
		}
	}
	generation := uint64(0)
	if rawGeneration, ok := msg.Values["enqueue_generation"]; ok {
		generation, err = strconv.ParseUint(fmt.Sprint(rawGeneration), 10, 64)
		if err != nil {
			idCopy := id
			return Delivery{}, &PoisonDeliveryError{
				Backend: "redis", Source: q.key, DeliveryID: msg.ID,
				RawEnvelope: rawEnvelope, FailureClass: "decode", Reason: fmt.Sprintf("parse enqueue_generation: %v", err),
				TaskID: &idCopy,
			}
		}
	}
	return Delivery{ID: msg.ID, TaskID: id, Generation: generation, Backend: "redis", Source: q.key, RawEnvelope: rawEnvelope}, nil
}

// parseDelivery is kept for focused queue tests that do not need queue identity.
func parseDelivery(msg redis.XMessage) (Delivery, error) {
	q := &RedisTaskQueue{key: "test:queue"}
	return q.parseRedisDelivery(msg)
}

func (q *RedisTaskQueue) Ack(ctx context.Context, deliveryID string) error {
	return q.client.XAck(ctx, q.key, q.group, deliveryID).Err()
}

func (q *RedisTaskQueue) Stats(ctx context.Context) (Stats, error) {
	if err := q.ensureGroup(ctx); err != nil {
		return Stats{}, err
	}

	length, err := q.client.XLen(ctx, q.key).Result()
	if err != nil {
		return Stats{}, err
	}
	groups, err := q.client.XInfoGroups(ctx, q.key).Result()
	if err != nil {
		return Stats{}, err
	}
	for _, group := range groups {
		if group.Name == q.group {
			return Stats{
				StreamLength: length,
				Pending:      group.Pending,
				Lag:          group.Lag,
			}, nil
		}
	}
	return Stats{}, fmt.Errorf("redis stream consumer group %q not found", q.group)
}
