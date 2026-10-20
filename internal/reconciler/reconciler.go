package reconciler

import (
	"context"
	"log"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type TaskRepository interface {
	RecoverStaleRunning(context.Context, time.Time, time.Time) (int64, error)
	FindReadyPending(context.Context, time.Time, int) ([]domain.Task, error)
	ClaimTaskEnqueue(context.Context, uint64, uint64, time.Time, time.Time) (repository.EnqueueClaim, repository.EnqueueClaimState, error)
	ConfirmTaskEnqueued(context.Context, repository.EnqueueClaim) error
	ReleaseTaskEnqueue(context.Context, repository.EnqueueClaim) error
}

type Reconciler struct {
	repo         TaskRepository
	queue        queue.TaskQueue
	interval     time.Duration
	staleRunning time.Duration
	batchSize    int
	coordinator  Coordinator
	leaseTTL     time.Duration
	enqueueLease time.Duration
}

func New(repo TaskRepository, q queue.TaskQueue, interval, staleRunning time.Duration) *Reconciler {
	return &Reconciler{repo: repo, queue: q, interval: interval, staleRunning: staleRunning, batchSize: 100, enqueueLease: 10 * time.Second}
}

func (r *Reconciler) ConfigureCoordinator(coordinator Coordinator, leaseTTL time.Duration) {
	r.coordinator = coordinator
	if leaseTTL <= 0 {
		leaseTTL = 15 * time.Second
	}
	r.leaseTTL = leaseTTL
}

func (r *Reconciler) ConfigureEnqueueLease(lease time.Duration) {
	if lease > 0 {
		r.enqueueLease = lease
	}
}

func (r *Reconciler) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		var lease CoordinationLease
		defer func() {
			if lease != nil {
				lease.Release()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lease = r.runCycle(ctx, lease)
			}
		}
	}()
}

func (r *Reconciler) runCycle(ctx context.Context, lease CoordinationLease) CoordinationLease {
	if r.coordinator == nil {
		r.runOnce(ctx)
		return nil
	}
	if lease != nil && lease.Context().Err() != nil {
		lease.Release()
		lease = nil
	}
	if lease == nil {
		acquiredLease, acquired, err := r.coordinator.TryAcquire(ctx, r.leaseTTL)
		if err != nil {
			appmetrics.ReconcilerCoordination.WithLabelValues("error").Inc()
			log.Printf("reconciler coordination acquire: %v", err)
			return nil
		}
		if !acquired {
			appmetrics.ReconcilerCoordination.WithLabelValues("contended").Inc()
			return nil
		}
		lease = acquiredLease
		appmetrics.ReconcilerCoordination.WithLabelValues("acquired").Inc()
	}

	r.runOnce(lease.Context())
	return lease
}

func (r *Reconciler) runOnce(ctx context.Context) {
	now := time.Now()
	recovered, err := r.repo.RecoverStaleRunning(ctx, now, now.Add(-r.staleRunning))
	if err != nil {
		log.Printf("reconciler recover stale running: %v", err)
		return
	}
	if recovered > 0 {
		appmetrics.ReconcilerRecovered.Add(float64(recovered))
	}

	tasks, err := r.repo.FindReadyPending(ctx, time.Now(), r.batchSize)
	if err != nil {
		log.Printf("reconciler scan pending: %v", err)
		return
	}
	for _, task := range tasks {
		now := time.Now()
		claim, state, err := r.repo.ClaimTaskEnqueue(ctx, task.ID, 0, now, now.Add(r.enqueueLease))
		if err != nil {
			appmetrics.EnqueueFencing.WithLabelValues("reconciler", "error").Inc()
			log.Printf("reconciler enqueue fence task %d: %v", task.ID, err)
			return
		}
		appmetrics.EnqueueFencing.WithLabelValues("reconciler", string(state)).Inc()
		if state != repository.EnqueueClaimAcquired {
			continue
		}
		if err := r.queue.Push(ctx, queue.Message{TaskID: claim.TaskID, Generation: claim.Generation}); err != nil {
			settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = r.repo.ReleaseTaskEnqueue(settleCtx, claim)
			cancel()
			log.Printf("reconciler enqueue task %d: %v", task.ID, err)
			return
		}
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		confirmErr := r.repo.ConfirmTaskEnqueued(settleCtx, claim)
		cancel()
		if confirmErr != nil {
			log.Printf("reconciler confirm enqueue task %d generation %d: %v", task.ID, claim.Generation, confirmErr)
			return
		}
		appmetrics.ReconcilerEnqueued.Inc()
		appmetrics.EnqueueConfirmed.WithLabelValues("reconciler").Inc()
	}
}
