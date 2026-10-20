package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/testsupport"
	"github.com/yfj898/taskflow-go/internal/verification"
)

func TestVerificationMySQLAtomicityAndGeneration(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_MYSQL_INTEGRATION_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("requires explicit *_ci or *_test database")
	}
	db, err := NewMySQL(dsn, 16, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.Task{}, &domain.OutboxEvent{}); err != nil {
		t.Fatal(err)
	}
	repo := NewTaskRepository(db)
	if err := repo.MigrateVerificationExplicit(); err != nil {
		t.Fatal(err)
	}
	if err := repo.VerifyVerificationSchema(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)
	in := domain.VerificationRequest{RunID: "d05fb8d9-655c-4c20-8b2d-" + suffix, ProjectID: "verification-test-" + suffix, TargetCommit: "0123456789012345678901234567890123456789", CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"backend-test", "frontend-test", "frontend-build"}, ExternalSnapshotID: "snapshot-" + suffix}
	const n = 12
	var wg sync.WaitGroup
	results := make(chan *domain.VerificationRun, n)
	errs := make(chan error, n)
	created := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, fresh, err := repo.CreateVerification(ctx, in, verification.Profile{ID: "verify-change", Revision: "1"})
			if err != nil {
				errs <- err
				return
			}
			results <- run
			created <- fresh
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	close(created)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	freshCount := 0
	for fresh := range created {
		if fresh {
			freshCount++
		}
	}
	if freshCount != 1 {
		t.Fatalf("fresh creates=%d want1", freshCount)
	}
	var id uint64
	for run := range results {
		if id == 0 {
			id = run.TaskID
		}
		if id != run.TaskID {
			t.Fatal("duplicate tasks for UUID")
		}
	}
	var taskCount, outboxCount, runCount int64
	db.Model(&domain.Task{}).Where("name = ?", "verification "+in.RunID).Count(&taskCount)
	db.Model(&domain.OutboxEvent{}).Where("task_id = ?", id).Count(&outboxCount)
	db.Model(&domain.VerificationRun{}).Where("run_id = ?", in.RunID).Count(&runCount)
	if taskCount != 1 || outboxCount != 1 || runCount != 1 {
		t.Fatalf("not atomic/deduplicated: task=%d outbox=%d run=%d", taskCount, outboxCount, runCount)
	}
	reordered := in
	reordered.CheckIDs = []string{"frontend-build", "backend-test", "frontend-test"}
	if same, fresh, err := repo.CreateVerification(ctx, reordered, verification.Profile{ID: "verify-change", Revision: "2"}); err != nil || fresh || same.TaskID != id {
		t.Fatalf("accepted run changed with registry/order: %+v %v %v", same, fresh, err)
	}
	bad := in
	bad.TargetCommit = "1123456789012345678901234567890123456789"
	if _, _, err := repo.CreateVerification(ctx, bad, verification.Profile{ID: "verify-change", Revision: "1"}); !errors.Is(err, ErrVerificationConflict) {
		t.Fatalf("conflict accepted: %v", err)
	}
	if ok, err := repo.Claim(ctx, id, 1, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatal("claim", ok, err)
	}
	task, err := repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	report := &domain.VerificationReport{Generation: 1, StartedAt: time.Now(), Checks: []domain.VerificationCheck{{ID: "backend-test", Required: true, Status: "not_run"}}}
	if err := repo.BeginVerificationAttempt(ctx, task, report); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&domain.Task{}).Where("id = ?", id).Update("lease_until", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := repo.RecoverStaleRunning(ctx, time.Now(), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.RenewVerificationLease(ctx, id, 1, time.Now().Add(time.Hour)); err != nil || ok {
		t.Fatalf("old generation renewed: %v %v", ok, err)
	}
	if ok, err := repo.Claim(ctx, id, 2, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatal("claim recovered", ok, err)
	}
	now := time.Now()
	report.FinishedAt = &now
	report.Verdict = "passed"
	report.Checks[0].Status = "passed"
	if ok, err := repo.SettleVerification(ctx, id, report, false); err != nil || ok {
		t.Fatalf("old generation settled: %v %v", ok, err)
	}
	task, err = repo.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	report.Generation = 2
	report.Verdict = "failed"
	report.Error = "real assertion"
	report.Checks[0].Status = "failed"
	if err := repo.BeginVerificationAttempt(ctx, task, report); err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.SettleVerification(ctx, id, report, false); err != nil || !ok {
		t.Fatalf("current generation rejected: %v %v", ok, err)
	}
	view, err := repo.VerificationView(ctx, in.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.TaskFailed || view.Generation != 2 || len(view.Attempts) != 2 || view.Attempts[0].Verdict != "incomplete" || view.Attempts[1].Verdict != "failed" {
		t.Fatalf("report/task mismatch: %+v", view)
	}
	if ok, err := repo.SettleVerification(ctx, id, report, false); err != nil || ok {
		t.Fatal("terminal settled twice", ok, err)
	}
	visible, err := repo.ListPublicTasks(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range visible {
		if task.Type == domain.VerificationTaskType {
			t.Fatal("reserved type leaked")
		}
	}
}
