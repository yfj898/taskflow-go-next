package outbox

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type Dispatcher struct {
	repo         *repository.TaskRepository
	queue        queue.TaskQueue
	interval     time.Duration
	lease        time.Duration
	enqueueLease time.Duration
	timeout      time.Duration
	batch        int
}

func New(repo *repository.TaskRepository, q queue.TaskQueue, interval time.Duration) *Dispatcher {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Dispatcher{
		repo: repo, queue: q, interval: interval,
		lease: 10 * time.Second, enqueueLease: 10 * time.Second, timeout: 3 * time.Second, batch: 100,
	}
}

func (d *Dispatcher) ConfigureEnqueueLease(lease time.Duration) {
	if lease > 0 {
		d.enqueueLease = lease
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()

		d.drain(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.drain(ctx)
			}
		}
	}()
}

func (d *Dispatcher) drain(ctx context.Context) {
	const maxBatchesPerDrain = 10
	for batchNo := 0; batchNo < maxBatchesPerDrain; batchNo++ {
		now := time.Now()
		events, err := d.repo.ClaimOutboxBatch(ctx, now, now.Add(d.lease), d.batch)
		if errors.Is(err, repository.ErrOutboxNotFound) {
			return
		}
		if err != nil {
			log.Printf("outbox claim: %v", err)
			return
		}

		d.publishClaimed(ctx, events)

		if len(events) < d.batch {
			return
		}
	}
}

func (d *Dispatcher) publishClaimed(ctx context.Context, events []domain.OutboxEvent) {
	now := time.Now()
	claims := make([]repository.EnqueueClaim, 0, len(events))
	messages := make([]queue.Message, 0, len(events))
	publishIDs := make([]uint64, 0, len(events))
	visibleClaims := make([]repository.EnqueueClaim, 0, len(events))
	visibleIDs := make([]uint64, 0, len(events))
	obsoleteIDs := make([]uint64, 0, len(events))
	deferredIDs := make([]uint64, 0, len(events))

	for i := range events {
		claim, state, err := d.repo.ClaimTaskEnqueue(ctx, events[i].TaskID, 1, now, now.Add(d.enqueueLease))
		if err != nil {
			appmetrics.EnqueueFencing.WithLabelValues("outbox", "error").Inc()
			log.Printf("outbox enqueue fence task=%d: %v", events[i].TaskID, err)
			deferredIDs = append(deferredIDs, events[i].ID)
			continue
		}
		appmetrics.EnqueueFencing.WithLabelValues("outbox", string(state)).Inc()
		switch state {
		case repository.EnqueueClaimAcquired:
			claims = append(claims, claim)
			messages = append(messages, queue.Message{TaskID: claim.TaskID, Generation: claim.Generation})
			publishIDs = append(publishIDs, events[i].ID)
		case repository.EnqueueClaimAlreadyVisible:
			visibleClaims = append(visibleClaims, claim)
			visibleIDs = append(visibleIDs, events[i].ID)
		case repository.EnqueueClaimObsolete:
			obsoleteIDs = append(obsoleteIDs, events[i].ID)
		case repository.EnqueueClaimContended, repository.EnqueueClaimNotReady:
			deferredIDs = append(deferredIDs, events[i].ID)
		}
	}

	if len(deferredIDs) > 0 {
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := d.repo.ReleaseOutboxBatch(settleCtx, deferredIDs)
		cancel()
		if err != nil {
			log.Printf("outbox release deferred count=%d: %v", len(deferredIDs), err)
		}
	}
	if len(visibleIDs) > 0 {
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := d.repo.ConfirmTaskEnqueuedAndOutboxBatch(settleCtx, visibleClaims, visibleIDs, time.Now())
		cancel()
		if err != nil {
			log.Printf("outbox settle visible count=%d: %v", len(visibleIDs), err)
		} else {
			appmetrics.EnqueueConfirmed.WithLabelValues("outbox").Add(float64(len(visibleClaims)))
		}
	}
	if len(obsoleteIDs) > 0 {
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := d.repo.MarkOutboxPublishedBatch(settleCtx, obsoleteIDs, time.Now())
		cancel()
		if err != nil {
			log.Printf("outbox settle obsolete count=%d: %v", len(obsoleteIDs), err)
		}
	}
	if len(messages) == 0 {
		return
	}

	pubCtx, cancel := context.WithTimeout(ctx, d.timeout)
	publishErr := d.queue.PushBatch(pubCtx, messages)
	cancel()
	if publishErr != nil {
		settleCtx, settleCancel := context.WithTimeout(context.Background(), 2*time.Second)
		for _, claim := range claims {
			_ = d.repo.ReleaseTaskEnqueue(settleCtx, claim)
		}
		markErr := d.repo.MarkOutboxFailedBatch(settleCtx, publishIDs, publishErr)
		settleCancel()
		if markErr != nil {
			log.Printf("outbox mark failed batch first=%d count=%d: %v", publishIDs[0], len(publishIDs), markErr)
		} else {
			log.Printf("outbox publish batch first=%d count=%d: %v", publishIDs[0], len(publishIDs), publishErr)
		}
		return
	}

	settleCtx, settleCancel := context.WithTimeout(context.Background(), 2*time.Second)
	confirmErr := d.repo.ConfirmTaskEnqueuedAndOutboxBatch(settleCtx, claims, publishIDs, time.Now())
	settleCancel()
	if confirmErr != nil {
		// The broker may already contain these messages. The finite enqueue
		// lease preserves crash recovery; only this publish/confirm ambiguity
		// can cause a later duplicate, which is unavoidable without a broker DB
		// transaction.
		log.Printf("outbox confirm enqueue batch first=%d count=%d: %v", publishIDs[0], len(publishIDs), confirmErr)
		return
	}
	appmetrics.EnqueueConfirmed.WithLabelValues("outbox").Add(float64(len(claims)))
}
