package monitor

import (
	"context"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
)

func StartQueueCompaction(ctx context.Context, compactor queue.Compactor, interval time.Duration, policy queue.CompactionPolicy) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		compactQueue(ctx, compactor, policy)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				compactQueue(ctx, compactor, policy)
			}
		}
	}()
}

func compactQueue(ctx context.Context, compactor queue.Compactor, policy queue.CompactionPolicy) {
	compactCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := compactor.Compact(compactCtx, time.Now(), policy)
	if err != nil {
		appmetrics.QueueCompactionRuns.WithLabelValues("error").Inc()
		return
	}
	if result.Protected {
		appmetrics.QueueCompactionProtected.Inc()
	}
	if result.TrimmedEntries > 0 {
		appmetrics.QueueCompactionRuns.WithLabelValues("trimmed").Inc()
		appmetrics.QueueCompactionTrimmedEntries.Add(float64(result.TrimmedEntries))
		return
	}
	appmetrics.QueueCompactionRuns.WithLabelValues("noop").Inc()
}
