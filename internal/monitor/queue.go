package monitor

import (
	"context"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
)

func StartQueueStats(ctx context.Context, provider queue.StatsProvider, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	go func() {
		collectQueueStats(ctx, provider)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectQueueStats(ctx, provider)
			}
		}
	}()
}

func collectQueueStats(ctx context.Context, provider queue.StatsProvider) {
	statCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stats, err := provider.Stats(statCtx)
	if err != nil {
		appmetrics.QueueStatsFresh.Set(0)
		appmetrics.QueueStatsErrors.Inc()
		return
	}

	lag := stats.Lag
	backlogLag := lag
	if backlogLag < 0 {
		backlogLag = 0
	}

	appmetrics.QueueStreamLength.Set(float64(stats.StreamLength))
	appmetrics.QueuePending.Set(float64(stats.Pending))
	appmetrics.QueueLag.Set(float64(lag))
	appmetrics.QueueBacklog.Set(float64(stats.Pending + backlogLag))
	appmetrics.QueueStatsLastSuccessTimestamp.Set(float64(time.Now().Unix()))
	// Publish freshness last so a scaler never observes fresh=1 before the
	// associated queue gauges have been updated for the same collection.
	appmetrics.QueueStatsFresh.Set(1)
}
