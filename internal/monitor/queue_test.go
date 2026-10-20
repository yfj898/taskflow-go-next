package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
)

type queueStatsProviderStub struct {
	stats queue.Stats
	err   error
}

type queueCompactorStub struct {
	result queue.CompactionResult
	err    error
}

func (p queueStatsProviderStub) Stats(context.Context) (queue.Stats, error) {
	return p.stats, p.err
}

func (s queueCompactorStub) Compact(context.Context, time.Time, queue.CompactionPolicy) (queue.CompactionResult, error) {
	return s.result, s.err
}

func TestCollectQueueStatsPublishesFreshBacklog(t *testing.T) {
	collectQueueStats(context.Background(), queueStatsProviderStub{stats: queue.Stats{
		StreamLength: 14,
		Pending:      3,
		Lag:          7,
	}})

	if got := testutil.ToFloat64(appmetrics.QueueStreamLength); got != 14 {
		t.Fatalf("stream length=%v, want 14", got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueBacklog); got != 10 {
		t.Fatalf("backlog=%v, want 10", got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueStatsFresh); got != 1 {
		t.Fatalf("fresh=%v, want 1", got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueStatsLastSuccessTimestamp); got <= 0 {
		t.Fatalf("last success timestamp=%v, want positive", got)
	}
}

func TestCollectQueueStatsMarksStaleWithoutDestroyingLastGoodBacklog(t *testing.T) {
	collectQueueStats(context.Background(), queueStatsProviderStub{stats: queue.Stats{
		StreamLength: 9,
		Pending:      2,
		Lag:          4,
	}})
	lastGoodBacklog := testutil.ToFloat64(appmetrics.QueueBacklog)
	lastSuccess := testutil.ToFloat64(appmetrics.QueueStatsLastSuccessTimestamp)
	errorsBefore := testutil.ToFloat64(appmetrics.QueueStatsErrors)

	collectQueueStats(context.Background(), queueStatsProviderStub{err: errors.New("redis unavailable")})

	if got := testutil.ToFloat64(appmetrics.QueueStatsFresh); got != 0 {
		t.Fatalf("fresh=%v, want 0 after collection failure", got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueBacklog); got != lastGoodBacklog {
		t.Fatalf("backlog changed from last good %v to %v on collection failure", lastGoodBacklog, got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueStatsLastSuccessTimestamp); got != lastSuccess {
		t.Fatalf("last success timestamp changed from %v to %v on collection failure", lastSuccess, got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueStatsErrors); got != errorsBefore+1 {
		t.Fatalf("stats errors=%v, want %v", got, errorsBefore+1)
	}
}

func TestCollectQueueStatsRecoversFreshness(t *testing.T) {
	collectQueueStats(context.Background(), queueStatsProviderStub{err: errors.New("redis unavailable")})
	if got := testutil.ToFloat64(appmetrics.QueueStatsFresh); got != 0 {
		t.Fatalf("fresh=%v, want 0 before recovery", got)
	}

	collectQueueStats(context.Background(), queueStatsProviderStub{stats: queue.Stats{Pending: 1, Lag: -1}})
	if got := testutil.ToFloat64(appmetrics.QueueStatsFresh); got != 1 {
		t.Fatalf("fresh=%v, want 1 after recovery", got)
	}
	if got := testutil.ToFloat64(appmetrics.QueueBacklog); got != 1 {
		t.Fatalf("backlog=%v, want pending-only backlog 1 for negative lag", got)
	}
}

func TestCompactQueuePublishesTrimmedAndProtectedMetrics(t *testing.T) {
	trimmedBefore := testutil.ToFloat64(appmetrics.QueueCompactionTrimmedEntries)
	protectedBefore := testutil.ToFloat64(appmetrics.QueueCompactionProtected)
	runsBefore := testutil.ToFloat64(appmetrics.QueueCompactionRuns.WithLabelValues("trimmed"))

	compactQueue(context.Background(), queueCompactorStub{result: queue.CompactionResult{
		TrimmedEntries: 9,
		Protected:      true,
	}}, queue.CompactionPolicy{MaxRetainedEntries: 5})

	if got := testutil.ToFloat64(appmetrics.QueueCompactionTrimmedEntries); got != trimmedBefore+9 {
		t.Fatalf("trimmed entries=%v, want %v", got, trimmedBefore+9)
	}
	if got := testutil.ToFloat64(appmetrics.QueueCompactionProtected); got != protectedBefore+1 {
		t.Fatalf("protected runs=%v, want %v", got, protectedBefore+1)
	}
	if got := testutil.ToFloat64(appmetrics.QueueCompactionRuns.WithLabelValues("trimmed")); got != runsBefore+1 {
		t.Fatalf("trimmed runs=%v, want %v", got, runsBefore+1)
	}
}

func TestCompactQueuePublishesErrorMetric(t *testing.T) {
	errorsBefore := testutil.ToFloat64(appmetrics.QueueCompactionRuns.WithLabelValues("error"))
	compactQueue(context.Background(), queueCompactorStub{err: errors.New("redis unavailable")}, queue.CompactionPolicy{})
	if got := testutil.ToFloat64(appmetrics.QueueCompactionRuns.WithLabelValues("error")); got != errorsBefore+1 {
		t.Fatalf("compaction error runs=%v, want %v", got, errorsBefore+1)
	}
}
