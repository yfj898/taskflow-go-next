package monitor

import (
	"context"
	"log"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type ControlPlaneSIEMHistoryRepository interface {
	PurgeControlPlaneSIEMDeliveryHistory(context.Context, time.Time, time.Time, int) (int, int, error)
}

func StartControlPlaneSIEMHistoryLifecycle(
	ctx context.Context,
	repo ControlPlaneSIEMHistoryRepository,
	interval, deliveredRetention, replayedDLQRetention time.Duration,
	batch int,
) {
	if repo == nil {
		return
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if deliveredRetention <= 0 {
		deliveredRetention = 30 * 24 * time.Hour
	}
	if replayedDLQRetention <= 0 {
		replayedDLQRetention = 90 * 24 * time.Hour
	}
	if batch <= 0 {
		batch = 1000
	}
	go func() {
		runControlPlaneSIEMHistoryLifecycle(ctx, repo, deliveredRetention, replayedDLQRetention, batch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runControlPlaneSIEMHistoryLifecycle(ctx, repo, deliveredRetention, replayedDLQRetention, batch)
			}
		}
	}()
}

func runControlPlaneSIEMHistoryLifecycle(
	ctx context.Context,
	repo ControlPlaneSIEMHistoryRepository,
	deliveredRetention, replayedDLQRetention time.Duration,
	batch int,
) {
	now := time.Now()
	delivered, replayedDLQ, err := repo.PurgeControlPlaneSIEMDeliveryHistory(
		ctx,
		now.Add(-deliveredRetention),
		now.Add(-replayedDLQRetention),
		batch,
	)
	if err != nil {
		appmetrics.ControlPlaneSIEMLifecycle.WithLabelValues("purge_failed").Inc()
		log.Printf("purge control-plane SIEM delivery history: %v", err)
		return
	}
	if delivered == 0 && replayedDLQ == 0 {
		appmetrics.ControlPlaneSIEMLifecycle.WithLabelValues("idle").Inc()
		return
	}
	if delivered > 0 {
		appmetrics.ControlPlaneSIEMLifecycle.WithLabelValues("purged_delivered").Add(float64(delivered))
		appmetrics.ControlPlaneSIEMPurged.WithLabelValues("delivered").Add(float64(delivered))
	}
	if replayedDLQ > 0 {
		appmetrics.ControlPlaneSIEMLifecycle.WithLabelValues("purged_replayed_dlq").Add(float64(replayedDLQ))
		appmetrics.ControlPlaneSIEMPurged.WithLabelValues("replayed_dlq").Add(float64(replayedDLQ))
	}
}
