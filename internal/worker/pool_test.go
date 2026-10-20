package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/queue"
)

type blockingDrainQueue struct {
	started chan struct{}
	once    sync.Once
}

func (q *blockingDrainQueue) Push(context.Context, queue.Message) error        { return nil }
func (q *blockingDrainQueue) PushBatch(context.Context, []queue.Message) error { return nil }
func (q *blockingDrainQueue) Ack(context.Context, string) error                { return nil }

func (q *blockingDrainQueue) Pop(ctx context.Context) (queue.Delivery, error) {
	q.once.Do(func() { close(q.started) })
	<-ctx.Done()
	return queue.Delivery{}, ctx.Err()
}

func TestPoolNormalizesInvalidSize(t *testing.T) {
	p := NewPool(0, nil, nil, nil, 0, 0)
	if p.size != 1 {
		t.Fatalf("pool size = %d, want 1", p.size)
	}
	if p.maxRetries != 1 {
		t.Fatalf("maxRetries = %d, want 1", p.maxRetries)
	}
	if p.lease != 3*time.Second {
		t.Fatalf("lease = %s, want 3s", p.lease)
	}
}

func TestRetryBackoffIsBounded(t *testing.T) {
	if got := retryBackoff(0); got != time.Second {
		t.Fatalf("retryBackoff(0) = %s", got)
	}
	if got := retryBackoff(99); got != 32*time.Second {
		t.Fatalf("retryBackoff(99) = %s", got)
	}
}

func TestBeginDrainStopsQueueIntake(t *testing.T) {
	q := &blockingDrainQueue{started: make(chan struct{})}
	p := NewPool(2, q, nil, nil, 1, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	select {
	case <-q.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start queue intake")
	}

	p.BeginDrain()
	if !p.IsDraining() {
		t.Fatal("pool did not enter draining state")
	}

	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain did not cancel blocked queue intake")
	}
	if p.Active() != 0 {
		t.Fatalf("active=%d, want 0", p.Active())
	}
}
