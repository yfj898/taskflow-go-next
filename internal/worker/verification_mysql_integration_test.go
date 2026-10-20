package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/testsupport"
	"github.com/yfj898/taskflow-go/internal/verification"
)

func TestVerificationWorkerMySQLRealCommands(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("verification runtime is Linux")
	}
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_MYSQL_INTEGRATION_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("requires *_ci or *_test database")
	}
	db, err := repository.NewMySQL(dsn, 10, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.Task{}, &domain.OutboxEvent{}); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewTaskRepository(db)
	if err := repo.MigrateVerificationExplicit(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	source := filepath.Join(root, "repo")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, b)
		}
		return strings.TrimSpace(string(b))
	}
	git("init")
	git("config", "user.name", "test")
	git("config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "data.txt"), []byte("versioned-data\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "data.txt")
	git("commit", "-m", "test baseline")
	commit := git("rev-parse", "HEAD")
	trueCmd, _ := exec.LookPath("true")
	falseCmd, _ := exec.LookPath("false")
	envCmd, _ := exec.LookPath("env")
	cfg := &verification.Config{WorkRoot: filepath.Join(root, "work"), ArtifactRoot: filepath.Join(root, "artifacts"), Projects: []verification.Project{{ID: "worker-tests", Repository: source, Subjects: []string{"svc"}, Profile: verification.Profile{ID: "verify-change", Revision: "1", Checks: []verification.Check{{ID: "required-check", Required: true, Argv: []string{trueCmd}}, {ID: "environment", Required: true, Argv: []string{envCmd}}}}}}}
	t.Setenv("TASKFLOW_TEST_SECRET", "must-not-be-inherited")
	router := executor.NewRouter(executor.NewDemoExecutor())
	router.Register(domain.VerificationTaskType, executor.NewVerificationExecutor(cfg, repo))
	p := NewPool(1, nil, repo, router, 3, 3*time.Second)
	for index, success := range []bool{true, false} {
		cfg.Projects[0].Profile.Checks[0].Argv = []string{trueCmd}
		if !success {
			cfg.Projects[0].Profile.Checks[0].Argv = []string{falseCmd}
		}
		suffix := fmt.Sprintf("%012x", (time.Now().UnixNano()+int64(index))&0xffffffffffff)
		input := domain.VerificationRequest{RunID: "d05fb8d9-655c-4c20-8b2d-" + suffix, ProjectID: "worker-tests", TargetCommit: commit, CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"required-check", "environment"}}
		run, _, err := repo.CreateVerification(context.Background(), input, cfg.Projects[0].Profile)
		if err != nil {
			t.Fatal(err)
		}
		acked, poison := p.execute(context.Background(), 1, queue.Delivery{TaskID: run.TaskID, Generation: 1})
		if !acked || poison != nil {
			t.Fatalf("not settled ack=%v poison=%+v", acked, poison)
		}
		view, err := repo.VerificationView(context.Background(), run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		want := domain.TaskFailed
		if success {
			want = domain.TaskSuccess
		}
		if view.Status != want || len(view.Attempts) != 1 || view.Generation != 1 {
			t.Fatalf("unexpected view: %+v", view)
		}
		task, err := repo.Get(context.Background(), run.TaskID)
		if err != nil || task.RetryCount != 0 || task.Result != "" {
			t.Fatalf("failed check retried/used Task.Result: %+v %v", task, err)
		}
		if view.Attempts[0].Checks[1].Status != "passed" {
			t.Fatalf("did not continue fixed checks: %+v", view.Attempts[0])
		}
		log, err := os.ReadFile(filepath.Join(cfg.ArtifactRoot, input.RunID, "1", "check-1.log"))
		if err != nil || strings.Contains(string(log), "must-not-be-inherited") {
			t.Fatalf("environment leak: %v", err)
		}
		var audits int64
		if db.Migrator().HasTable(&domain.AIRouteAudit{}) {
			db.Model(&domain.AIRouteAudit{}).Where("task_id = ?", run.TaskID).Count(&audits)
			if audits != 0 {
				t.Fatal("verification persisted AI audit")
			}
		}
		// The payload remains valid JSON, not a report transport.
		var stored domain.VerificationRequest
		if err := json.Unmarshal([]byte(task.Payload), &stored); err != nil || stored.TargetCommit != commit {
			t.Fatal("payload changed", err)
		}
	}
}

type verificationShutdownRace struct {
	cancel  context.CancelFunc
	verdict string
}

func (e *verificationShutdownRace) Execute(ctx context.Context, task *domain.Task) (string, error) {
	r, err := e.ExecuteDetailed(ctx, task)
	return r.Result, err
}
func (e *verificationShutdownRace) ExecuteDetailed(_ context.Context, task *domain.Task) (executor.ExecutionResult, error) {
	now := time.Now()
	status := "passed"
	code := 0
	var err error
	if e.verdict == "failed" {
		status = "failed"
		code = 1
		err = executor.Permanent(fmt.Errorf("actual assertion exited 1"))
	}
	report := &domain.VerificationReport{Generation: task.EnqueueGeneration, StartedAt: now, FinishedAt: &now, Verdict: e.verdict, Checks: []domain.VerificationCheck{{ID: "required", Required: true, Status: status, ExitCode: &code}}}
	e.cancel()
	return executor.ExecutionResult{Verification: report}, err
}
func TestVerificationWorkerShutdownDoesNotRetryCompletedResult(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_MYSQL_INTEGRATION_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("requires *_ci or *_test database")
	}
	db, err := repository.NewMySQL(dsn, 10, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.Task{}, &domain.OutboxEvent{}); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewTaskRepository(db)
	if err := repo.MigrateVerificationExplicit(); err != nil {
		t.Fatal(err)
	}
	for _, verdict := range []string{"failed", "passed"} {
		t.Run(verdict, func(t *testing.T) {
			suffix := fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)
			in := domain.VerificationRequest{RunID: "d05fb8d9-655c-4c20-8b2d-" + suffix, ProjectID: "shutdown-race", TargetCommit: strings.Repeat("a", 40), CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"required"}}
			run, _, err := repo.CreateVerification(context.Background(), in, verification.Profile{ID: "verify-change", Revision: "1"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := NewPool(1, nil, repo, &verificationShutdownRace{cancel: cancel, verdict: verdict}, 3, 3*time.Second)
			if acked, poison := p.execute(ctx, 1, queue.Delivery{TaskID: run.TaskID, Generation: 1}); !acked || poison != nil {
				t.Fatal("completed result not settled", acked, poison)
			}
			view, err := repo.VerificationView(context.Background(), run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.TaskFailed
			if verdict == "passed" {
				want = domain.TaskSuccess
			}
			if view.Status != want || view.Generation != 1 || view.Attempts[0].Verdict != verdict {
				t.Fatalf("shutdown retried completed result: %+v", view)
			}
		})
	}
}
