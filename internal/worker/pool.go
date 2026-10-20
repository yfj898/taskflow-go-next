package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Pool struct {
	size       int
	queue      queue.TaskQueue
	repo       *repository.TaskRepository
	exec       executor.Executor
	policy     AIRoutingPolicyProvider
	budget     AIBudgetAdmission
	maxRetries int
	lease      time.Duration
	wg         sync.WaitGroup

	intakeMu     sync.Mutex
	intakeCancel context.CancelFunc
	draining     atomic.Bool
	active       atomic.Int64
}

type AIRoutingPolicyProvider interface {
	CurrentAIRoutingPolicy() executor.AIRoutingPolicySnapshot
}

type AIBudgetAdmission interface {
	ReserveForTask(context.Context, *domain.Task) (*domain.AIBudgetReservation, error)
}

func NewPool(size int, q queue.TaskQueue, repo *repository.TaskRepository, exec executor.Executor, maxRetries int, lease time.Duration) *Pool {
	if size <= 0 {
		size = 1
	}
	if maxRetries <= 0 {
		maxRetries = 1
	}
	if lease < 3*time.Second {
		lease = 3 * time.Second
	}
	return &Pool{size: size, queue: q, repo: repo, exec: exec, maxRetries: maxRetries, lease: lease}
}

func (p *Pool) ConfigureAIRoutingPolicyProvider(policy AIRoutingPolicyProvider) {
	p.policy = policy
}

func (p *Pool) ConfigureAIBudgetAdmission(budget AIBudgetAdmission) {
	p.budget = budget
}

func (p *Pool) Start(ctx context.Context) {
	intakeCtx, intakeCancel := context.WithCancel(ctx)
	p.intakeMu.Lock()
	p.intakeCancel = intakeCancel
	if p.draining.Load() {
		intakeCancel()
	}
	p.intakeMu.Unlock()

	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.runWorker(intakeCtx, ctx, i+1)
	}
}

func (p *Pool) Wait() { p.wg.Wait() }

// BeginDrain fences queue intake without canceling work that has already been
// claimed. The execution context stays alive until the process-level drain
// deadline expires, allowing in-flight work to settle normally.
func (p *Pool) BeginDrain() {
	if !p.draining.CompareAndSwap(false, true) {
		return
	}
	p.intakeMu.Lock()
	if p.intakeCancel != nil {
		p.intakeCancel()
	}
	p.intakeMu.Unlock()
}

func (p *Pool) IsDraining() bool { return p.draining.Load() }

func (p *Pool) Active() int64 { return p.active.Load() }

func (p *Pool) runWorker(intakeCtx, executionCtx context.Context, workerID int) {
	defer p.wg.Done()
	for {
		select {
		case <-intakeCtx.Done():
			return
		default:
		}

		delivery, err := p.queue.Pop(intakeCtx)
		if err != nil {
			var poison *queue.PoisonDeliveryError
			if errors.As(err, &poison) {
				if p.quarantinePoison(poison) {
					if ackErr := p.queue.Ack(context.Background(), poison.DeliveryID); ackErr != nil {
						appmetrics.QueuePoisonDeliveries.WithLabelValues(poison.Backend, poison.FailureClass, "ack_error").Inc()
					}
				}
				continue
			}
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		p.active.Add(1)
		appmetrics.WorkerInflight.Inc()
		acked, poison := p.execute(executionCtx, workerID, delivery)
		appmetrics.WorkerInflight.Dec()
		p.active.Add(-1)
		if poison != nil {
			acked = p.quarantinePoison(poison)
		}
		if acked {
			_ = p.queue.Ack(context.Background(), delivery.ID)
		}
	}
}

// execute returns true only when the message can be ACKed. Database errors are
// deliberately left unacked so Redis Streams can redeliver them. A structurally
// valid delivery that references no durable task is returned as poison rather
// than being silently discarded as a duplicate.
func (p *Pool) execute(ctx context.Context, workerID int, delivery queue.Delivery) (bool, *queue.PoisonDeliveryError) {
	taskID, generation := delivery.TaskID, delivery.Generation
	claimed, err := p.repo.Claim(ctx, taskID, generation, time.Now().Add(p.lease))
	if err != nil {
		return false, nil
	}
	if !claimed {
		_, getErr := p.repo.Get(ctx, taskID)
		if errors.Is(getErr, repository.ErrTaskNotFound) {
			taskIDCopy := taskID
			generationCopy := generation
			return false, &queue.PoisonDeliveryError{
				Backend: delivery.Backend, Source: delivery.Source, DeliveryID: delivery.ID,
				RawEnvelope: delivery.RawEnvelope, FailureClass: "orphan", Reason: "queue delivery references missing task",
				TaskID: &taskIDCopy, Generation: &generationCopy,
			}
		}
		if getErr != nil {
			return false, nil
		}
		// Duplicate delivery: another worker already claimed or settled this task,
		// or an old Phase 16 generation arrived late.
		return true, nil
	}

	task, err := p.repo.Get(ctx, taskID)
	if err != nil {
		return false, nil
	}

	execBaseCtx := ctx
	if task.TraceParent != "" {
		carrier := propagation.MapCarrier{"traceparent": task.TraceParent}
		if task.TraceState != "" {
			carrier["tracestate"] = task.TraceState
		}
		execBaseCtx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	}
	execCtx, taskSpan := otel.Tracer("taskflow/worker").Start(
		execBaseCtx,
		"task.execute",
		trace.WithAttributes(
			attribute.String("task.id", strconv.FormatUint(task.ID, 10)),
			attribute.String("task.type", task.Type),
		),
	)
	execCtx, cancelExec := context.WithCancel(execCtx)
	if p.policy != nil {
		execCtx = executor.WithAIRoutingPolicy(execCtx, p.policy.CurrentAIRoutingPolicy())
	}
	heartbeatDone := make(chan error, 1)
	go p.heartbeat(execCtx, cancelExec, taskID, task.Type, generation, heartbeatDone)
	var reservation *domain.AIBudgetReservation
	var execution executor.ExecutionResult
	var execErr error
	if p.budget != nil && task.Type != domain.VerificationTaskType {
		reservation, execErr = p.budget.ReserveForTask(execCtx, task)
	}
	if execErr == nil {
		execution, execErr = executor.RunDetailed(p.exec, execCtx, task)
	}
	result := execution.Result
	metadataJSON := execution.MetadataJSON
	if metadataJSON == "" {
		metadataJSON = task.ExecutionMetadata
	}
	var routeAudit *domain.AIRouteAudit
	if metadataJSON != "" && task.Type != domain.VerificationTaskType {
		routeAudit = &domain.AIRouteAudit{
			TaskID:       task.ID,
			Attempt:      task.RetryCount,
			MetadataJSON: metadataJSON,
		}
	}
	cancelExec()
	if heartbeatErr := <-heartbeatDone; heartbeatErr != nil && execErr == nil {
		execErr = fmt.Errorf("worker lease lost: %w", heartbeatErr)
	}
	if execErr != nil {
		taskSpan.RecordError(execErr)
	}
	taskSpan.End()
	if task.Type == domain.VerificationTaskType {
		report := execution.Verification
		if report == nil {
			now := time.Now().UTC()
			report = &domain.VerificationReport{Generation: generation, StartedAt: now, FinishedAt: &now, Verdict: "incomplete", Checks: []domain.VerificationCheck{}}
			if execErr == nil {
				execErr = errors.New("verification executor returned no structured report")
			}
		}
		if execErr != nil {
			report.Error = execErr.Error()
			if report.Verdict == "passed" {
				report.Verdict = "incomplete"
			}
		}
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Shutdown only recovers incomplete work. A completed assertion failure
		// (or success) remains terminal even when shutdown races its settlement.
		recoverPending := ctx.Err() != nil && report.Verdict == "incomplete"
		updated, err := p.repo.SettleVerification(settleCtx, taskID, report, recoverPending)
		if err == nil && updated {
			outcome := "failed"
			if recoverPending {
				outcome = "retry"
			} else if report.Verdict == "passed" {
				outcome = "success"
			}
			appmetrics.TasksProcessed.WithLabelValues(outcome).Inc()
		}
		return err == nil && updated, nil
	}

	if execErr == nil {
		actualUSD, actualTokens := aiBudgetUsage(metadataJSON)
		updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(ctx, taskID, domain.TaskRunning, domain.TaskSuccess, map[string]any{
			"result":      result + " by worker " + strconv.Itoa(workerID),
			"last_error":  "",
			"lease_until": nil,
		}, routeAudit, reservation, true, actualUSD, actualTokens)
		if err != nil || !updated {
			return false, nil
		}
		if reservation != nil {
			appmetrics.AIBudgetSettledUSD.Add(actualUSD)
			appmetrics.AIBudgetSettledTokens.Add(float64(actualTokens))
			if actualUSD > reservation.ReservedUSD+1e-9 {
				appmetrics.AIBudgetOverruns.WithLabelValues("usd").Inc()
			}
			if actualTokens > reservation.ReservedTokens {
				appmetrics.AIBudgetOverruns.WithLabelValues("tokens").Inc()
			}
		}
		appmetrics.TasksProcessed.WithLabelValues("success").Inc()
		return true, nil
	}

	settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Only a canceled parent context means the process is shutting down. A
	// downstream RPC deadline is a normal execution failure and must consume a
	// retry budget instead of being re-enqueued forever.
	if ctx.Err() != nil {
		updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(settleCtx, taskID, domain.TaskRunning, domain.TaskPending, map[string]any{
			"last_error":  ctx.Err().Error(),
			"next_run_at": time.Now(),
			"lease_until": nil,
		}, routeAudit, reservation, false, 0, 0)
		return err == nil && updated, nil
	}

	if !executor.IsRetryable(execErr) {
		updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(settleCtx, taskID, domain.TaskRunning, domain.TaskFailed, map[string]any{
			"last_error":  execErr.Error(),
			"lease_until": nil,
		}, routeAudit, reservation, false, 0, 0)
		if err == nil && updated {
			appmetrics.TasksProcessed.WithLabelValues("failed").Inc()
		}
		return err == nil && updated, nil
	}

	if task.RetryCount < p.maxRetries {
		delay := retryBackoff(task.RetryCount)
		updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(settleCtx, taskID, domain.TaskRunning, domain.TaskPending, map[string]any{
			"retry_count": task.RetryCount + 1,
			"last_error":  execErr.Error(),
			"next_run_at": time.Now().Add(delay),
			"lease_until": nil,
		}, routeAudit, reservation, false, 0, 0)
		if err == nil && updated {
			appmetrics.TasksProcessed.WithLabelValues("retry").Inc()
		}
		return err == nil && updated, nil
	}

	updated, err := p.repo.TransitionWithAIRouteAuditAndBudget(settleCtx, taskID, domain.TaskRunning, domain.TaskFailed, map[string]any{
		"last_error":  execErr.Error(),
		"lease_until": nil,
	}, routeAudit, reservation, false, 0, 0)
	if err == nil && updated {
		appmetrics.TasksProcessed.WithLabelValues("failed").Inc()
	}
	return err == nil && updated, nil
}

func (p *Pool) quarantinePoison(poison *queue.PoisonDeliveryError) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	message := &domain.DeadLetterMessage{
		SourceBackend:     poison.Backend,
		SourceQueue:       poison.Source,
		SourceDeliveryID:  poison.DeliveryID,
		FailureClass:      poison.FailureClass,
		FailureReason:     poison.Reason,
		RawEnvelope:       poison.RawEnvelope,
		TaskID:            poison.TaskID,
		EnqueueGeneration: poison.Generation,
		ReplayState:       domain.DeadLetterQuarantined,
	}
	if err := p.repo.QuarantineDeadLetter(ctx, message); err != nil {
		appmetrics.QueuePoisonDeliveries.WithLabelValues(poison.Backend, poison.FailureClass, "quarantine_error").Inc()
		return false
	}
	appmetrics.QueuePoisonDeliveries.WithLabelValues(poison.Backend, poison.FailureClass, "quarantined").Inc()
	return true
}

func aiBudgetUsage(metadataJSON string) (float64, int64) {
	if metadataJSON == "" {
		return 0, 0
	}
	var audit executor.AIRouteDecisionAudit
	if err := json.Unmarshal([]byte(metadataJSON), &audit); err != nil || audit.Kind != "ai_route" {
		return 0, 0
	}
	return audit.ActualCostUSD, int64(audit.TotalTokens)
}

func (p *Pool) heartbeat(ctx context.Context, cancel context.CancelFunc, taskID uint64, taskType string, generation uint64, done chan<- error) {
	interval := p.lease / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			hbCtx, hbCancel := context.WithTimeout(context.Background(), interval)
			var renewed bool
			var err error
			if taskType == domain.VerificationTaskType {
				renewed, err = p.repo.RenewVerificationLease(hbCtx, taskID, generation, time.Now().Add(p.lease))
			} else {
				renewed, err = p.repo.RenewLease(hbCtx, taskID, time.Now().Add(p.lease))
			}
			hbCancel()
			if err != nil {
				cancel()
				done <- err
				return
			}
			if !renewed {
				cancel()
				done <- errors.New("task no longer owned by worker")
				return
			}
		}
	}
}

func retryBackoff(retryCount int) time.Duration {
	seconds := 1 << min(retryCount, 5)
	return time.Duration(seconds) * time.Second
}
