package monitor

import (
	"context"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type DeadLetterLifecyclePolicy struct {
	ReplayedRetention    time.Duration
	QuarantinedRetention time.Duration
	ReplayAuditRetention time.Duration
	MaxEntries           int
	BatchSize            int
	ArchiveBatchSize     int
	ArchiveMinCopies     int
}

type DeadLetterLifecycleRepository interface {
	ListDeadLetterArchiveCandidates(context.Context, time.Time, time.Time, int, int) ([]uint64, error)
	RunDeadLetterLifecycle(context.Context, time.Time, time.Time, time.Time, time.Time, int, int, int) (repository.DeadLetterLifecycleResult, error)
	DeadLetterLifecycleStats(context.Context) ([]repository.DeadLetterLifecycleStat, error)
}

type DeadLetterArchiver interface {
	Archive(context.Context, uint64) (*domain.DeadLetterArchive, error)
}

func StartDeadLetterLifecycle(ctx context.Context, repo DeadLetterLifecycleRepository, archiver DeadLetterArchiver, interval time.Duration, policy DeadLetterLifecyclePolicy) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		collectDeadLetterLifecycle(ctx, repo, archiver, policy)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectDeadLetterLifecycle(ctx, repo, archiver, policy)
			}
		}
	}()
}

func collectDeadLetterLifecycle(ctx context.Context, repo DeadLetterLifecycleRepository, archiver DeadLetterArchiver, policy DeadLetterLifecyclePolicy) {
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	now := time.Now()
	var replayedBefore, quarantinedBefore, auditBefore time.Time
	if policy.ReplayedRetention > 0 {
		replayedBefore = now.Add(-policy.ReplayedRetention)
	}
	if policy.QuarantinedRetention > 0 {
		quarantinedBefore = now.Add(-policy.QuarantinedRetention)
	}
	if policy.ReplayAuditRetention > 0 {
		auditBefore = now.Add(-policy.ReplayAuditRetention)
	}
	verificationStarted := time.Now()
	archiveBatch := policy.ArchiveBatchSize
	if archiveBatch <= 0 {
		archiveBatch = 50
	}
	candidates, err := repo.ListDeadLetterArchiveCandidates(runCtx, replayedBefore, quarantinedBefore, policy.MaxEntries, archiveBatch)
	if err != nil {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("archive_candidates_error").Inc()
		return
	}
	archiveErrors := 0
	if archiver != nil {
		for _, id := range candidates {
			if _, err := archiver.Archive(runCtx, id); err != nil {
				archiveErrors++
				if runCtx.Err() != nil {
					break
				}
			}
		}
	} else if len(candidates) > 0 {
		archiveErrors = len(candidates)
	}
	if archiveErrors > 0 {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("archive_error").Inc()
	}

	result, err := repo.RunDeadLetterLifecycle(runCtx, replayedBefore, quarantinedBefore, auditBefore, verificationStarted, policy.ArchiveMinCopies, policy.MaxEntries, policy.BatchSize)
	if err != nil {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("error").Inc()
		return
	}

	if result.RetentionPurged > 0 {
		appmetrics.QueueDeadLetterPurged.WithLabelValues("retention").Add(float64(result.RetentionPurged))
	}
	if result.CapacityPurged > 0 {
		appmetrics.QueueDeadLetterPurged.WithLabelValues("capacity").Add(float64(result.CapacityPurged))
	}
	if result.AuditPurged > 0 {
		appmetrics.QueueDeadLetterReplayAuditsPurged.Add(float64(result.AuditPurged))
	}
	appmetrics.QueueDeadLetterOverLimit.Set(float64(result.OverLimit))
	if result.RetentionPurged+result.CapacityPurged+result.AuditPurged > 0 {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("purged").Inc()
	} else if result.OverLimit > 0 {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("protected").Inc()
	} else {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("noop").Inc()
	}

	stats, err := repo.DeadLetterLifecycleStats(runCtx)
	if err != nil {
		appmetrics.QueueDeadLetterLifecycleRuns.WithLabelValues("stats_error").Inc()
		return
	}
	appmetrics.QueueDeadLetters.Reset()
	appmetrics.QueueDeadLetterOldestAge.Reset()
	for _, stat := range stats {
		labels := []string{stat.FailureClass, string(stat.ReplayState)}
		appmetrics.QueueDeadLetters.WithLabelValues(labels...).Set(float64(stat.Count))
		age := 0.0
		if !stat.OldestAt.IsZero() {
			age = now.Sub(stat.OldestAt).Seconds()
			if age < 0 {
				age = 0
			}
		}
		appmetrics.QueueDeadLetterOldestAge.WithLabelValues(labels...).Set(age)
	}
}
