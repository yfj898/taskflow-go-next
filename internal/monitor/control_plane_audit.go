package monitor

import (
	"context"
	"log"
	"time"

	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/service"
)

func StartControlPlaneAuditLifecycle(
	ctx context.Context,
	audits *service.ControlPlaneAuditService,
	interval time.Duration,
	retention time.Duration,
	batch int,
) {
	if audits == nil {
		return
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if retention <= 0 {
		retention = 90 * 24 * time.Hour
	}
	if batch <= 0 {
		batch = 1000
	}
	go func() {
		runControlPlaneAuditLifecycle(ctx, audits, retention, batch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runControlPlaneAuditLifecycle(ctx, audits, retention, batch)
			}
		}
	}()
}

func runControlPlaneAuditLifecycle(ctx context.Context, audits *service.ControlPlaneAuditService, retention time.Duration, batch int) {
	sealed, err := audits.SealLegacy(ctx, batch)
	if err != nil {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("seal_failed").Inc()
		log.Printf("seal legacy control-plane audits: %v", err)
		return
	}
	if sealed > 0 {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("sealed").Add(float64(sealed))
	}

	report, err := audits.Verify(ctx)
	if err != nil {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("verify_failed").Inc()
		appmetrics.ControlPlaneAuditIntegrity.Set(0)
		log.Printf("verify control-plane audit chain: %v", err)
		return
	}
	if !report.Valid {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("integrity_failure").Inc()
		appmetrics.ControlPlaneAuditIntegrity.Set(0)
		log.Printf("control-plane audit integrity failure: %s", report.Failure)
		return
	}
	appmetrics.ControlPlaneAuditIntegrity.Set(1)

	purged, err := audits.PurgeBefore(ctx, time.Now().Add(-retention), batch)
	if err != nil {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("purge_failed").Inc()
		log.Printf("purge retained control-plane audits: %v", err)
		return
	}
	if purged == 0 && sealed == 0 {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("idle").Inc()
		return
	}
	if purged > 0 {
		appmetrics.ControlPlaneAuditLifecycle.WithLabelValues("purged").Add(float64(purged))
		appmetrics.ControlPlaneAuditPurged.Add(float64(purged))
	}
}
