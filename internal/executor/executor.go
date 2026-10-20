package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type Executor interface {
	Execute(ctx context.Context, task *domain.Task) (string, error)
}

// ExecutionResult carries optional structured execution metadata alongside the
// user-visible result. MetadataJSON is intentionally opaque to the generic
// worker so executor-specific details can evolve without coupling the queue
// core to AI routing fields.
type ExecutionResult struct {
	Result       string
	MetadataJSON string
	Verification *domain.VerificationReport
}

// DetailedExecutor is an optional extension implemented by executors that can
// return audit metadata. Existing executors only need to implement Executor.
type DetailedExecutor interface {
	ExecuteDetailed(ctx context.Context, task *domain.Task) (ExecutionResult, error)
}

// RunDetailed preserves backwards compatibility with plain Executor
// implementations while allowing the worker and gRPC layer to capture richer
// execution metadata when available.
func RunDetailed(exec Executor, ctx context.Context, task *domain.Task) (ExecutionResult, error) {
	if detailed, ok := exec.(DetailedExecutor); ok {
		return detailed.ExecuteDetailed(ctx, task)
	}
	result, err := exec.Execute(ctx, task)
	return ExecutionResult{Result: result}, err
}

type executionError struct {
	err       error
	retryable bool
}

func (e *executionError) Error() string { return e.err.Error() }
func (e *executionError) Unwrap() error { return e.err }

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &executionError{err: err, retryable: true}
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &executionError{err: err, retryable: false}
}

func IsRetryable(err error) bool {
	var execErr *executionError
	if errors.As(err, &execErr) {
		return execErr.retryable
	}
	return true
}

// DemoExecutor keeps the project self-contained while exposing real retry
// behaviour. type=fail_once fails its first execution; always_fail never succeeds.
type DemoExecutor struct {
	mu     sync.Mutex
	failed map[uint64]struct{}
}

func NewDemoExecutor() *DemoExecutor {
	return &DemoExecutor{failed: make(map[uint64]struct{})}
}

func (e *DemoExecutor) Execute(ctx context.Context, task *domain.Task) (string, error) {
	delay := 150 * time.Millisecond
	if task.Type == "slow" {
		delay = 8 * time.Second
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(delay):
	}

	if task.Type == "always_fail" {
		return "", Permanent(errors.New("simulated permanent failure"))
	}
	if task.Type == "fail_once" {
		e.mu.Lock()
		_, seen := e.failed[task.ID]
		if !seen {
			e.failed[task.ID] = struct{}{}
		}
		e.mu.Unlock()
		if !seen {
			return "", Retryable(errors.New("simulated transient failure"))
		}
	}

	return fmt.Sprintf("task %d processed", task.ID), nil
}
