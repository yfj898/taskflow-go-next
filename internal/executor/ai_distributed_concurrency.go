package executor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrAIGlobalConcurrency = errors.New("AI global provider/model concurrency saturated")
var ErrAITenantConcurrency = errors.New("AI distributed tenant concurrency quota saturated")

func globalConcurrencyError(class, provider, model string) error {
	err := fmt.Errorf("%w: slo=%s provider=%s model=%s", ErrAIGlobalConcurrency, class, provider, model)
	if class == AISLORealtime {
		return Permanent(err)
	}
	return Retryable(err)
}

func tenantConcurrencyError(class, provider, model string) error {
	err := fmt.Errorf("%w: slo=%s provider=%s model=%s", ErrAITenantConcurrency, class, provider, model)
	if class == AISLORealtime {
		return Permanent(err)
	}
	return Retryable(err)
}

type DistributedConcurrencyLease interface {
	Release()
}

type DistributedConcurrencyCoordinator interface {
	Acquire(ctx context.Context, provider, model string, limit int, leaseTTL, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error)
}

type DistributedTenantConcurrencyCoordinator interface {
	AcquireTenant(ctx context.Context, provider, model, tenantID string, limit int, leaseTTL, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error)
}

type RedisConcurrencyCoordinator struct {
	client       *redis.Client
	prefix       string
	pollInterval time.Duration
}

func NewRedisConcurrencyCoordinator(client *redis.Client, prefix string, pollInterval time.Duration) *RedisConcurrencyCoordinator {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "taskflow:ai:concurrency"
	}
	if pollInterval <= 0 {
		pollInterval = 25 * time.Millisecond
	}
	return &RedisConcurrencyCoordinator{client: client, prefix: prefix, pollInterval: pollInterval}
}

func (c *RedisConcurrencyCoordinator) Acquire(ctx context.Context, provider, model string, limit int, leaseTTL, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error) {
	if c == nil || c.client == nil || limit <= 0 {
		return 0, 0, noopDistributedLease{}, nil
	}
	provider = normalizeConcurrencyKeyPart(provider)
	model = normalizeConcurrencyKeyPart(model)
	key := fmt.Sprintf("%s:%s:%s", c.prefix, provider, model)
	return c.acquireKey(ctx, key, limit, leaseTTL, maxWait, ErrAIGlobalConcurrency)
}

func (c *RedisConcurrencyCoordinator) AcquireTenant(ctx context.Context, provider, model, tenantID string, limit int, leaseTTL, maxWait time.Duration) (time.Duration, int, DistributedConcurrencyLease, error) {
	if c == nil || c.client == nil || limit <= 0 {
		return 0, 0, noopDistributedLease{}, nil
	}
	provider = normalizeConcurrencyKeyPart(provider)
	model = normalizeConcurrencyKeyPart(model)
	key := fmt.Sprintf("%s:tenant:%s:%s:%s", c.prefix, provider, model, tenantConcurrencyKeyPart(tenantID))
	return c.acquireKey(ctx, key, limit, leaseTTL, maxWait, ErrAITenantConcurrency)
}

func (c *RedisConcurrencyCoordinator) acquireKey(ctx context.Context, key string, limit int, leaseTTL, maxWait time.Duration, saturatedErr error) (time.Duration, int, DistributedConcurrencyLease, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	if maxWait < 0 {
		maxWait = 0
	}
	leaseID, err := newConcurrencyLeaseID()
	if err != nil {
		return 0, 0, nil, err
	}
	started := time.Now()
	deadline := started.Add(maxWait)
	for {
		now := time.Now()
		acquired, active, err := c.tryAcquire(ctx, key, leaseID, limit, leaseTTL, now)
		if err != nil {
			return time.Since(started), active, nil, err
		}
		if acquired {
			leaseCtx, cancel := context.WithCancel(context.Background())
			lease := &redisConcurrencyLease{coordinator: c, key: key, leaseID: leaseID, ttl: leaseTTL, cancel: cancel, done: make(chan struct{})}
			go lease.renewLoop(leaseCtx)
			return time.Since(started), active, lease, nil
		}
		if maxWait == 0 || !time.Now().Before(deadline) {
			return time.Since(started), active, nil, saturatedErr
		}
		remaining := time.Until(deadline)
		wait := c.pollInterval
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return time.Since(started), active, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

var acquireConcurrencyScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local count = redis.call('ZCARD', KEYS[1])
local limit = tonumber(ARGV[3])
if count >= limit then
  return {0, count}
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {1, count + 1}
`)

func (c *RedisConcurrencyCoordinator) tryAcquire(ctx context.Context, key, leaseID string, limit int, ttl time.Duration, now time.Time) (bool, int, error) {
	expiresAt := now.Add(ttl).UnixMilli()
	keyTTL := ttl * 2
	if keyTTL < time.Second {
		keyTTL = time.Second
	}
	result, err := acquireConcurrencyScript.Run(ctx, c.client, []string{key}, now.UnixMilli(), expiresAt, limit, leaseID, keyTTL.Milliseconds()).Result()
	if err != nil {
		return false, 0, fmt.Errorf("acquire distributed AI concurrency: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return false, 0, fmt.Errorf("unexpected distributed concurrency response %T", result)
	}
	acquired, err := redisResultInt(values[0])
	if err != nil {
		return false, 0, err
	}
	active, err := redisResultInt(values[1])
	if err != nil {
		return false, 0, err
	}
	return acquired == 1, active, nil
}

var renewConcurrencyScript = redis.NewScript(`
if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

var releaseConcurrencyScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
return redis.call('ZCARD', KEYS[1])
`)

type redisConcurrencyLease struct {
	coordinator *RedisConcurrencyCoordinator
	key         string
	leaseID     string
	ttl         time.Duration
	cancel      context.CancelFunc
	done        chan struct{}
	once        sync.Once
}

func (l *redisConcurrencyLease) renewLoop(ctx context.Context) {
	defer close(l.done)
	interval := l.ttl / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			keyTTL := l.ttl * 2
			renewCtx, cancel := context.WithTimeout(context.Background(), interval)
			_, _ = renewConcurrencyScript.Run(renewCtx, l.coordinator.client, []string{l.key}, l.leaseID, now.Add(l.ttl).UnixMilli(), keyTTL.Milliseconds()).Result()
			cancel()
		}
	}
}

func (l *redisConcurrencyLease) Release() {
	if l == nil || l.coordinator == nil || l.coordinator.client == nil {
		return
	}
	l.once.Do(func() {
		l.cancel()
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = releaseConcurrencyScript.Run(ctx, l.coordinator.client, []string{l.key}, l.leaseID, time.Now().UnixMilli()).Result()
		cancel()
	})
}

type noopDistributedLease struct{}

func (noopDistributedLease) Release() {}

func normalizeConcurrencyKeyPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	value = strings.ReplaceAll(value, ":", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func tenantConcurrencyKeyPart(tenantID string) string {
	tenantID = normalizeTenantID(tenantID)
	sum := sha256.Sum256([]byte(tenantID))
	return hex.EncodeToString(sum[:8])
}

func newConcurrencyLeaseID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate concurrency lease id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func redisResultInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case int64:
		return int(v), nil
	case int:
		return v, nil
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}
