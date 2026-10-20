package monitor

import (
	"context"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/service"
)

type DeadLetterArchiveObjectPolicy struct {
	ObjectRetention time.Duration
	OrphanGrace     time.Duration
	DrillInterval   time.Duration
	GCBatchSize     int
	OrphanBatchSize int
	DrillBatchSize  int
}

func StartDeadLetterArchiveObjectLifecycle(
	ctx context.Context,
	repo *repository.TaskRepository,
	deadLetters *service.DeadLetterService,
	interval time.Duration,
	policy DeadLetterArchiveObjectPolicy,
) {
	if interval <= 0 {
		interval = time.Hour
	}
	go func() {
		collectDeadLetterArchiveObjects(ctx, repo, deadLetters, policy)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectDeadLetterArchiveObjects(ctx, repo, deadLetters, policy)
			}
		}
	}()
}

func collectDeadLetterArchiveObjects(
	ctx context.Context,
	repo *repository.TaskRepository,
	deadLetters *service.DeadLetterService,
	policy DeadLetterArchiveObjectPolicy,
) {
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if policy.OrphanGrace > 0 {
		deadLetters.ReconcileArchiveOrphans(runCtx, policy.OrphanGrace, policy.OrphanBatchSize)
	}

	if policy.ObjectRetention > 0 {
		batch := policy.GCBatchSize
		if batch <= 0 {
			batch = 50
		}
		gcBefore := time.Now().Add(-policy.ObjectRetention)
		archives, err := repo.ListDeadLetterArchiveObjectGCCandidates(runCtx, gcBefore, batch)
		if err != nil {
			appmetrics.QueueDeadLetterArchiveObjectGC.WithLabelValues("candidate_error").Inc()
		} else {
			for _, archive := range archives {
				if err := deadLetters.GCArchiveObject(runCtx, archive.ID, gcBefore); err != nil {
					appmetrics.QueueDeadLetterArchiveObjectGC.WithLabelValues("error").Inc()
				}
				if runCtx.Err() != nil {
					break
				}
			}
		}
	}

	if policy.DrillInterval > 0 {
		batch := policy.DrillBatchSize
		if batch <= 0 {
			batch = 3
		}
		archives, err := repo.ListDeadLetterArchiveDrillCandidates(runCtx, time.Now().Add(-policy.DrillInterval), batch)
		if err != nil {
			appmetrics.QueueDeadLetterArchiveDrills.WithLabelValues("candidate_error").Inc()
			return
		}
		for _, archive := range archives {
			_ = deadLetters.DrillArchive(runCtx, archive.ID)
			if runCtx.Err() != nil {
				break
			}
		}
	}
}
