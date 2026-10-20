package executor

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func newBufconnExecutor(t *testing.T) *GRPCExecutor {
	return newBufconnExecutorWith(t, NewDemoExecutor())
}

func newBufconnExecutorWith(t *testing.T, backend Executor) *GRPCExecutor {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	rpc.RegisterExecutorServiceServer(server, NewGRPCServer(backend))
	healthServer := health.NewServer()
	healthServer.SetServingStatus(rpc.ExecutorServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewGRPCExecutorWithConn(conn, time.Second)
}

type grpcMetadataExecutor struct {
	fail bool
}

func (e *grpcMetadataExecutor) Execute(ctx context.Context, task *domain.Task) (string, error) {
	result, err := e.ExecuteDetailed(ctx, task)
	return result.Result, err
}

func (e *grpcMetadataExecutor) ExecuteDetailed(context.Context, *domain.Task) (ExecutionResult, error) {
	result := ExecutionResult{Result: "metadata-result", MetadataJSON: `{"kind":"ai_route","selected_provider":"p1"}`}
	if e.fail {
		return result, Retryable(context.DeadlineExceeded)
	}
	return result, nil
}

func TestGRPCExecutorSuccessAndHealth(t *testing.T) {
	exec := newBufconnExecutor(t)
	if err := exec.Ready(context.Background()); err != nil {
		t.Fatalf("ready: %v", err)
	}
	result, err := exec.Execute(context.Background(), &domain.Task{ID: 7, Name: "demo", Type: "compute"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result != "task 7 processed" {
		t.Fatalf("result=%q", result)
	}
}

func TestGRPCExecutorErrorClassification(t *testing.T) {
	exec := newBufconnExecutor(t)
	_, err := exec.Execute(context.Background(), &domain.Task{ID: 8, Type: "fail_once"})
	if err == nil || !IsRetryable(err) {
		t.Fatalf("fail_once error=%v retryable=%v", err, IsRetryable(err))
	}
	_, err = exec.Execute(context.Background(), &domain.Task{ID: 9, Type: "always_fail"})
	if err == nil || IsRetryable(err) {
		t.Fatalf("always_fail error=%v retryable=%v", err, IsRetryable(err))
	}
}

func TestGRPCExecutorRetriesTransientTask(t *testing.T) {
	exec := newBufconnExecutor(t)
	task := &domain.Task{ID: 10, Type: "fail_once"}
	_, _ = exec.Execute(context.Background(), task)
	result, err := exec.Execute(context.Background(), task)
	if err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if result != "task 10 processed" {
		t.Fatalf("result=%q", result)
	}
}

func TestGRPCExecutorDeadlineIsRetryable(t *testing.T) {
	exec := newBufconnExecutor(t)
	exec.timeout = 20 * time.Millisecond
	_, err := exec.Execute(context.Background(), &domain.Task{ID: 11, Type: "slow"})
	if err == nil || !IsRetryable(err) {
		t.Fatalf("deadline error=%v retryable=%v", err, IsRetryable(err))
	}
}

func TestGRPCExecutorTaskSpecificTimeout(t *testing.T) {
	exec := newBufconnExecutor(t)
	exec.timeout = 5 * time.Second
	exec.ConfigureTaskTimeout(AITaskType, 35*time.Second)

	if got := exec.timeoutFor(" AI_GENERATE "); got != 35*time.Second {
		t.Fatalf("AI timeout = %s, want 35s", got)
	}
	if got := exec.timeoutFor("compute"); got != 5*time.Second {
		t.Fatalf("compute timeout = %s, want 5s", got)
	}
}

func TestGRPCExecutorPropagatesExecutionMetadataOnSuccess(t *testing.T) {
	exec := newBufconnExecutorWith(t, &grpcMetadataExecutor{})
	task := &domain.Task{ID: 12, Type: AITaskType}
	result, err := exec.Execute(context.Background(), task)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result != "metadata-result" {
		t.Fatalf("result=%q", result)
	}
	if task.ExecutionMetadata != `{"kind":"ai_route","selected_provider":"p1"}` {
		t.Fatalf("metadata=%q", task.ExecutionMetadata)
	}
}

func TestGRPCExecutorPropagatesExecutionMetadataOnError(t *testing.T) {
	exec := newBufconnExecutorWith(t, &grpcMetadataExecutor{fail: true})
	task := &domain.Task{ID: 13, Type: AITaskType}
	_, err := exec.Execute(context.Background(), task)
	if err == nil || !IsRetryable(err) {
		t.Fatalf("error=%v retryable=%v", err, IsRetryable(err))
	}
	if task.ExecutionMetadata != `{"kind":"ai_route","selected_provider":"p1"}` {
		t.Fatalf("metadata=%q", task.ExecutionMetadata)
	}
}
