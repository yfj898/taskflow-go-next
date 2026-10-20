package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type fakeRepository struct {
	mu        sync.Mutex
	recovered int
	finds     int
	tasks     []domain.Task
	visible   map[uint64]uint64
}

func (r *fakeRepository) RecoverStaleRunning(context.Context, time.Time, time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recovered++
	return 0, nil
}

func (r *fakeRepository) FindReadyPending(context.Context, time.Time, int) ([]domain.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finds++
	return append([]domain.Task(nil), r.tasks...), nil
}

func (r *fakeRepository) ClaimTaskEnqueue(_ context.Context, taskID, expectedGeneration uint64, _ time.Time, _ time.Time) (repository.EnqueueClaim, repository.EnqueueClaimState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var generation uint64 = 1
	for _, task := range r.tasks {
		if task.ID == taskID && task.EnqueueGeneration > 0 {
			generation = task.EnqueueGeneration
			break
		}
	}
	if expectedGeneration > 0 && expectedGeneration != generation {
		return repository.EnqueueClaim{TaskID: taskID, Generation: generation}, repository.EnqueueClaimObsolete, nil
	}
	if r.visible != nil && r.visible[taskID] >= generation {
		return repository.EnqueueClaim{TaskID: taskID, Generation: generation}, repository.EnqueueClaimAlreadyVisible, nil
	}
	return repository.EnqueueClaim{TaskID: taskID, Generation: generation}, repository.EnqueueClaimAcquired, nil
}

func (r *fakeRepository) ConfirmTaskEnqueued(_ context.Context, claim repository.EnqueueClaim) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.visible == nil {
		r.visible = make(map[uint64]uint64)
	}
	r.visible[claim.TaskID] = claim.Generation
	return nil
}

func (*fakeRepository) ReleaseTaskEnqueue(context.Context, repository.EnqueueClaim) error { return nil }

type fakeQueue struct {
	mu     sync.Mutex
	pushes []queue.Message
}

func (q *fakeQueue) Push(_ context.Context, message queue.Message) error {
	q.mu.Lock()
	q.pushes = append(q.pushes, message)
	q.mu.Unlock()
	return nil
}

func (*fakeQueue) PushBatch(context.Context, []queue.Message) error { return nil }
func (*fakeQueue) Pop(context.Context) (queue.Delivery, error) {
	return queue.Delivery{}, errors.New("unused")
}
func (*fakeQueue) Ack(context.Context, string) error { return nil }

type sharedFakeCoordinator struct {
	mu    sync.Mutex
	held  bool
	tries int
}

func (c *sharedFakeCoordinator) TryAcquire(parent context.Context, _ time.Duration) (CoordinationLease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tries++
	if c.held {
		return nil, false, nil
	}
	c.held = true
	ctx, cancel := context.WithCancel(parent)
	return &fakeLease{ctx: ctx, cancel: cancel, release: func() {
		c.mu.Lock()
		c.held = false
		c.mu.Unlock()
	}}, true, nil
}

type fakeLease struct {
	ctx     context.Context
	cancel  context.CancelFunc
	release func()
	once    sync.Once
}

func (l *fakeLease) Context() context.Context { return l.ctx }
func (l *fakeLease) Release() {
	l.once.Do(func() {
		l.cancel()
		l.release()
	})
}

func TestDistributedCoordinationRunsOnlyLeader(t *testing.T) {
	coord := &sharedFakeCoordinator{}
	repoOne := &fakeRepository{tasks: []domain.Task{{ID: 11}}}
	repoTwo := &fakeRepository{tasks: []domain.Task{{ID: 11}}}
	queueOne := &fakeQueue{}
	queueTwo := &fakeQueue{}
	one := New(repoOne, queueOne, time.Second, time.Minute)
	two := New(repoTwo, queueTwo, time.Second, time.Minute)
	one.ConfigureCoordinator(coord, 5*time.Second)
	two.ConfigureCoordinator(coord, 5*time.Second)

	leaseOne := one.runCycle(context.Background(), nil)
	if leaseOne == nil {
		t.Fatal("first reconciler did not acquire leadership")
	}
	leaseTwo := two.runCycle(context.Background(), nil)
	if leaseTwo != nil {
		t.Fatal("second reconciler acquired leadership while first lease was held")
	}
	if repoOne.finds != 1 || len(queueOne.pushes) != 1 {
		t.Fatalf("leader work = finds %d pushes %v, want one run", repoOne.finds, queueOne.pushes)
	}
	if repoTwo.finds != 0 || len(queueTwo.pushes) != 0 {
		t.Fatalf("contender performed reconciliation: finds %d pushes %v", repoTwo.finds, queueTwo.pushes)
	}

	leaseOne.Release()
	leaseTwo = two.runCycle(context.Background(), nil)
	if leaseTwo == nil {
		t.Fatal("second reconciler did not acquire after leader released")
	}
	leaseTwo.Release()
	if repoTwo.finds != 1 || len(queueTwo.pushes) != 1 {
		t.Fatalf("failover leader work = finds %d pushes %v, want one run", repoTwo.finds, queueTwo.pushes)
	}
}

func TestLeaderLeasePersistsAcrossCycles(t *testing.T) {
	coord := &sharedFakeCoordinator{}
	repo := &fakeRepository{}
	r := New(repo, &fakeQueue{}, time.Second, time.Minute)
	r.ConfigureCoordinator(coord, 5*time.Second)

	lease := r.runCycle(context.Background(), nil)
	lease = r.runCycle(context.Background(), lease)
	lease = r.runCycle(context.Background(), lease)
	if lease == nil {
		t.Fatal("leader lease unexpectedly disappeared")
	}
	lease.Release()
	if coord.tries != 1 {
		t.Fatalf("leadership acquire attempts = %d, want 1 while lease remains healthy", coord.tries)
	}
	if repo.finds != 3 {
		t.Fatalf("leader reconciliation cycles = %d, want 3", repo.finds)
	}
}

func TestRepeatedPendingScanPublishesGenerationOnce(t *testing.T) {
	repo := &fakeRepository{tasks: []domain.Task{{ID: 42, EnqueueGeneration: 3}}}
	q := &fakeQueue{}
	r := New(repo, q, time.Second, time.Minute)

	r.runOnce(context.Background())
	r.runOnce(context.Background())
	r.runOnce(context.Background())

	if len(q.pushes) != 1 {
		t.Fatalf("pushes=%v, want one broker publish for generation", q.pushes)
	}
	if q.pushes[0].TaskID != 42 || q.pushes[0].Generation != 3 {
		t.Fatalf("unexpected message: %+v", q.pushes[0])
	}
}

func TestStartReleasesLeadershipWhenContextIsCanceled(t *testing.T) {
	coord := &sharedFakeCoordinator{}
	r := New(&fakeRepository{}, &fakeQueue{}, 5*time.Millisecond, time.Minute)
	r.ConfigureCoordinator(coord, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for {
		coord.mu.Lock()
		held := coord.held
		coord.mu.Unlock()
		if held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconciler never acquired leadership")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	deadline = time.Now().Add(time.Second)
	for {
		coord.mu.Lock()
		held := coord.held
		coord.mu.Unlock()
		if !held {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("reconciler did not release leadership after context cancellation")
		}
		time.Sleep(time.Millisecond)
	}
}
