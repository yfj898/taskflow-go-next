package executor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/rpc"
	apptelemetry "github.com/yfj898/taskflow-go/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type GRPCExecutor struct {
	conn         *grpc.ClientConn
	client       rpc.ExecutorServiceClient
	health       grpc_health_v1.HealthClient
	timeout      time.Duration
	timeoutMu    sync.RWMutex
	taskTimeouts map[string]time.Duration
	breaker      *CircuitBreaker
}

func NewGRPCExecutor(target string, timeout time.Duration) (*GRPCExecutor, error) {
	if target == "" {
		return nil, errors.New("executor grpc target is required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}
	return NewGRPCExecutorWithConn(conn, timeout), nil
}

func NewGRPCExecutorWithConn(conn *grpc.ClientConn, timeout time.Duration) *GRPCExecutor {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &GRPCExecutor{
		conn:         conn,
		client:       rpc.NewExecutorServiceClient(conn),
		health:       grpc_health_v1.NewHealthClient(conn),
		timeout:      timeout,
		taskTimeouts: make(map[string]time.Duration),
		breaker:      NewCircuitBreaker(5, 5*time.Second),
	}
}

func (e *GRPCExecutor) ConfigureCircuitBreaker(threshold int, openFor time.Duration) {
	e.breaker = NewCircuitBreaker(threshold, openFor)
}

func (e *GRPCExecutor) ConfigureTaskTimeout(taskType string, timeout time.Duration) {
	taskType = strings.ToLower(strings.TrimSpace(taskType))
	if taskType == "" || timeout <= 0 {
		return
	}
	e.timeoutMu.Lock()
	e.taskTimeouts[taskType] = timeout
	e.timeoutMu.Unlock()
}

func (e *GRPCExecutor) timeoutFor(taskType string) time.Duration {
	taskType = strings.ToLower(strings.TrimSpace(taskType))
	e.timeoutMu.RLock()
	timeout, ok := e.taskTimeouts[taskType]
	e.timeoutMu.RUnlock()
	if ok && timeout > 0 {
		return timeout
	}
	return e.timeout
}

func (e *GRPCExecutor) Close() error { return e.conn.Close() }

func (e *GRPCExecutor) Ready(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	resp, err := e.health.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{Service: rpc.ExecutorServiceName})
	if err != nil {
		return err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("executor grpc health=%s", resp.GetStatus())
	}
	return nil
}

func (e *GRPCExecutor) Execute(ctx context.Context, task *domain.Task) (string, error) {
	started := time.Now()
	defer func() { appmetrics.GRPCExecutorDuration.Observe(time.Since(started).Seconds()) }()
	traceCtx, span := otel.Tracer("taskflow/grpc-client").Start(
		ctx,
		"executor.Execute",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("task.id", strconv.FormatUint(task.ID, 10)),
			attribute.String("task.type", task.Type),
		),
	)
	defer span.End()
	if err := e.breaker.Allow(); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		appmetrics.GRPCExecutorCalls.WithLabelValues("circuit_open").Inc()
		return "", Retryable(err)
	}
	callCtx, cancel := context.WithTimeout(traceCtx, e.timeoutFor(task.Type))
	defer cancel()
	callCtx = apptelemetry.InjectGRPC(callCtx)

	requestFields := map[string]any{
		"id":          strconv.FormatUint(task.ID, 10),
		"name":        task.Name,
		"type":        task.Type,
		"payload":     task.Payload,
		"retry_count": float64(task.RetryCount),
	}
	if policy, ok := AIRoutingPolicyFromContext(ctx); ok {
		requestFields["routing_policy_version"] = strconv.FormatUint(policy.Version, 10)
		requestFields["routing_policy_strategy"] = policy.Strategy
		requestFields["routing_policy_default_output_tokens"] = float64(policy.DefaultOutputTokens)
		requestFields["routing_policy_tenant_quota_managed"] = policy.TenantQuotaManaged
		requestFields["routing_policy_tenant_quota_enabled"] = policy.TenantQuotaEnabled
		requestFields["routing_policy_tenant_quota_default_limit"] = float64(policy.TenantQuotaDefaultLimit)
		requestFields["routing_policy_tenant_quota_overrides"] = policy.TenantQuotaOverrides
	}
	req, err := structpb.NewStruct(requestFields)
	if err != nil {
		return "", Permanent(fmt.Errorf("build grpc request: %w", err))
	}

	resp, err := e.client.Execute(callCtx, req)
	if err != nil {
		if metadataJSON := metadataJSONFromGRPCError(err); metadataJSON != "" {
			task.ExecutionMetadata = metadataJSON
		}
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		if ctx.Err() != nil {
			e.breaker.Success()
			appmetrics.GRPCExecutorCalls.WithLabelValues("timeout_or_canceled").Inc()
			return "", ctx.Err()
		}
		if callCtx.Err() != nil {
			e.breaker.Failure()
			appmetrics.GRPCExecutorCalls.WithLabelValues("timeout_or_canceled").Inc()
			return "", Retryable(fmt.Errorf("executor grpc deadline: %w", callCtx.Err()))
		}
		st, ok := status.FromError(err)
		if !ok {
			e.breaker.Failure()
			appmetrics.GRPCExecutorCalls.WithLabelValues("retryable_error").Inc()
			return "", Retryable(err)
		}
		switch st.Code() {
		case codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated:
			e.breaker.Success()
			appmetrics.GRPCExecutorCalls.WithLabelValues("permanent_error").Inc()
			return "", Permanent(err)
		default:
			e.breaker.Failure()
			appmetrics.GRPCExecutorCalls.WithLabelValues("retryable_error").Inc()
			return "", Retryable(err)
		}
	}
	if metadata, ok := resp.Fields["metadata_json"]; ok {
		task.ExecutionMetadata = metadata.GetStringValue()
	}

	value, ok := resp.Fields["result"]
	if !ok {
		err := errors.New("grpc executor response missing result")
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		e.breaker.Success()
		appmetrics.GRPCExecutorCalls.WithLabelValues("invalid_response").Inc()
		return "", Permanent(err)
	}
	e.breaker.Success()
	appmetrics.GRPCExecutorCalls.WithLabelValues("success").Inc()
	return value.GetStringValue(), nil
}

func metadataJSONFromGRPCError(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, detail := range st.Details() {
		payload, ok := detail.(*structpb.Struct)
		if !ok {
			continue
		}
		if value, ok := payload.Fields["metadata_json"]; ok {
			return value.GetStringValue()
		}
	}
	return ""
}

type GRPCServer struct {
	exec Executor
}

func NewGRPCServer(exec Executor) *GRPCServer { return &GRPCServer{exec: exec} }

func (s *GRPCServer) Execute(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	appmetrics.ExecutorInflight.Inc()
	defer appmetrics.ExecutorInflight.Dec()
	ctx = apptelemetry.ExtractGRPC(ctx)
	ctx, span := otel.Tracer("taskflow/grpc-server").Start(
		ctx,
		"executor.Execute",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	fields := req.GetFields()
	id, err := strconv.ParseUint(fields["id"].GetStringValue(), 10, 64)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid task id")
		return nil, status.Error(codes.InvalidArgument, "invalid task id")
	}
	span.SetAttributes(
		attribute.String("task.id", strconv.FormatUint(id, 10)),
		attribute.String("task.type", fields["type"].GetStringValue()),
	)
	task := &domain.Task{
		ID:         id,
		Name:       fields["name"].GetStringValue(),
		Type:       fields["type"].GetStringValue(),
		Payload:    fields["payload"].GetStringValue(),
		RetryCount: int(fields["retry_count"].GetNumberValue()),
	}
	if strategy := fields["routing_policy_strategy"].GetStringValue(); strategy != "" {
		version, _ := strconv.ParseUint(fields["routing_policy_version"].GetStringValue(), 10, 64)
		ctx = WithAIRoutingPolicy(ctx, AIRoutingPolicySnapshot{
			Version:                 version,
			Strategy:                strategy,
			DefaultOutputTokens:     int(fields["routing_policy_default_output_tokens"].GetNumberValue()),
			TenantQuotaManaged:      fields["routing_policy_tenant_quota_managed"].GetBoolValue(),
			TenantQuotaEnabled:      fields["routing_policy_tenant_quota_enabled"].GetBoolValue(),
			TenantQuotaDefaultLimit: int(fields["routing_policy_tenant_quota_default_limit"].GetNumberValue()),
			TenantQuotaOverrides:    fields["routing_policy_tenant_quota_overrides"].GetStringValue(),
		})
	}

	execution, execErr := RunDetailed(s.exec, ctx, task)
	metadataJSON := execution.MetadataJSON
	if metadataJSON == "" {
		metadataJSON = task.ExecutionMetadata
	}
	if execErr != nil {
		span.RecordError(execErr)
		span.SetStatus(otelcodes.Error, execErr.Error())
		code := codes.FailedPrecondition
		if errors.Is(execErr, context.Canceled) {
			code = codes.Canceled
		} else if errors.Is(execErr, context.DeadlineExceeded) {
			code = codes.DeadlineExceeded
		} else if IsRetryable(execErr) {
			code = codes.Unavailable
		}
		return nil, grpcStatusWithMetadata(code, execErr.Error(), metadataJSON)
	}

	responseFields := map[string]any{"result": execution.Result}
	if metadataJSON != "" {
		responseFields["metadata_json"] = metadataJSON
	}
	resp, err := structpb.NewStruct(responseFields)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func grpcStatusWithMetadata(code codes.Code, message, metadataJSON string) error {
	st := status.New(code, message)
	if metadataJSON == "" {
		return st.Err()
	}
	detail, err := structpb.NewStruct(map[string]any{"metadata_json": metadataJSON})
	if err != nil {
		return st.Err()
	}
	withDetails, err := st.WithDetails(detail)
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}
