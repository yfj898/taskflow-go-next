package siem

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type LeadershipLease interface {
	Context() context.Context
	Fence() uint64
	Release()
}

type DeliveryCoordinator interface {
	TryAcquire(context.Context, time.Duration) (LeadershipLease, bool, error)
}

type RedisFencingCoordinator struct {
	client   *redis.Client
	lockKey  string
	fenceKey string
}

func NewRedisFencingCoordinator(client *redis.Client, lockKey string) *RedisFencingCoordinator {
	return &RedisFencingCoordinator{
		client:   client,
		lockKey:  lockKey,
		fenceKey: lockKey + ":fence",
	}
}

var acquireSIEMLeadershipScript = redis.NewScript(`
if redis.call("SETNX", KEYS[1], ARGV[1]) == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  local fence = redis.call("INCR", KEYS[2])
  return {1, fence}
end
return {0, 0}
`)

var renewSIEMLeadershipScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var releaseSIEMLeadershipScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (c *RedisFencingCoordinator) TryAcquire(parent context.Context, ttl time.Duration) (LeadershipLease, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, errors.New("SIEM Redis fencing coordinator is not configured")
	}
	if c.lockKey == "" {
		return nil, false, errors.New("SIEM coordination key is empty")
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	owner, err := randomLeaseToken()
	if err != nil {
		return nil, false, fmt.Errorf("generate SIEM leadership owner: %w", err)
	}
	result, err := acquireSIEMLeadershipScript.Run(
		parent,
		c.client,
		[]string{c.lockKey, c.fenceKey},
		owner,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return nil, false, fmt.Errorf("acquire SIEM leadership: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return nil, false, fmt.Errorf("unexpected SIEM leadership response %T", result)
	}
	acquired, err := redisInteger(values[0])
	if err != nil {
		return nil, false, err
	}
	if acquired != 1 {
		return nil, false, nil
	}
	fenceValue, err := redisInteger(values[1])
	if err != nil || fenceValue <= 0 {
		return nil, false, fmt.Errorf("invalid SIEM leadership fence %v", values[1])
	}

	leaseCtx, cancel := context.WithCancel(parent)
	lease := &redisFencingLease{
		client:  c.client,
		lockKey: c.lockKey,
		owner:   owner,
		fence:   uint64(fenceValue),
		ttl:     ttl,
		ctx:     leaseCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go lease.renewLoop()
	return lease, true, nil
}

type redisFencingLease struct {
	client  *redis.Client
	lockKey string
	owner   string
	fence   uint64
	ttl     time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

func (l *redisFencingLease) Context() context.Context { return l.ctx }
func (l *redisFencingLease) Fence() uint64            { return l.fence }

func (l *redisFencingLease) renewLoop() {
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
			result, err := renewSIEMLeadershipScript.Run(
				renewCtx,
				l.client,
				[]string{l.lockKey},
				l.owner,
				l.ttl.Milliseconds(),
			).Int()
			cancel()
			if err != nil || result != 1 {
				l.cancel()
				return
			}
		}
	}
}

func (l *redisFencingLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cancel()
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = releaseSIEMLeadershipScript.Run(ctx, l.client, []string{l.lockKey}, l.owner).Result()
		cancel()
	})
}

func redisInteger(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		var n int64
		if _, err := fmt.Sscan(v, &n); err != nil {
			return 0, err
		}
		return n, nil
	case []byte:
		var n int64
		if _, err := fmt.Sscan(string(v), &n); err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}
