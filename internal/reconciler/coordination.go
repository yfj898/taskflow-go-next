package reconciler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type CoordinationLease interface {
	Context() context.Context
	Release()
}

type Coordinator interface {
	TryAcquire(context.Context, time.Duration) (CoordinationLease, bool, error)
}

type RedisCoordinator struct {
	client *redis.Client
	key    string
}

func NewRedisCoordinator(client *redis.Client, key string) *RedisCoordinator {
	return &RedisCoordinator{client: client, key: key}
}

func (c *RedisCoordinator) TryAcquire(parent context.Context, ttl time.Duration) (CoordinationLease, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, errors.New("reconciler Redis coordinator is not configured")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	owner, err := newOwnerToken()
	if err != nil {
		return nil, false, err
	}
	ok, err := c.client.SetNX(parent, c.key, owner, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	leaseCtx, cancel := context.WithCancel(parent)
	lease := &redisCoordinationLease{
		client: c.client,
		key:    c.key,
		owner:  owner,
		ttl:    ttl,
		ctx:    leaseCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go lease.renewLoop()
	return lease, true, nil
}

var renewLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var releaseLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

type redisCoordinationLease struct {
	client *redis.Client
	key    string
	owner  string
	ttl    time.Duration
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (l *redisCoordinationLease) Context() context.Context { return l.ctx }

func (l *redisCoordinationLease) renewLoop() {
	defer close(l.done)
	interval := l.ttl / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), interval)
			result, err := renewLeaseScript.Run(renewCtx, l.client, []string{l.key}, l.owner, l.ttl.Milliseconds()).Int()
			cancel()
			if err != nil || result != 1 {
				appmetrics.ReconcilerCoordination.WithLabelValues("lost").Inc()
				l.cancel()
				return
			}
		}
	}
}

func (l *redisCoordinationLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cancel()
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = releaseLeaseScript.Run(ctx, l.client, []string{l.key}, l.owner).Result()
		cancel()
	})
}

func newOwnerToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate reconciler owner token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
