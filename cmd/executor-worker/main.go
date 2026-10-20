package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/yfj898/taskflow-go/internal/config"
	"github.com/yfj898/taskflow-go/internal/executor"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/rpc"
	apptelemetry "github.com/yfj898/taskflow-go/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg := config.Load()
	appmetrics.ProcessDraining.Set(0)
	shutdownTracing, err := apptelemetry.Init(context.Background(), "taskflow-executor", cfg.OTelLogSpans)
	if err != nil {
		log.Fatalf("init telemetry: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.ExecutorGRPCListen)
	if err != nil {
		log.Fatalf("listen executor grpc: %v", err)
	}

	server := grpc.NewServer()
	var distributedCoordinator executor.DistributedConcurrencyCoordinator
	redisClient := queue.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer redisClient.Close()
	coordinator := executor.NewRedisConcurrencyCoordinator(redisClient, cfg.AIConcurrencyRedisPrefix, 25*time.Millisecond)
	var tenantCoordinator executor.DistributedTenantConcurrencyCoordinator = coordinator
	if cfg.AIGlobalConcurrencyLimit > 0 || cfg.AIFallbackGlobalLimit > 0 || cfg.AITenantQuotaEnabled {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := redisClient.Ping(pingCtx).Err(); err != nil {
			pingCancel()
			log.Fatalf("distributed AI concurrency Redis unavailable: %v", err)
		}
		pingCancel()
		if cfg.AIGlobalConcurrencyLimit > 0 || cfg.AIFallbackGlobalLimit > 0 {
			distributedCoordinator = coordinator
		}
	}
	taskExecutor := executor.NewTaskExecutorWithRouting(
		time.Duration(cfg.AITimeoutMS)*time.Millisecond,
		executor.AIRoutingConfig{
			Strategy:            cfg.AIRoutingStrategy,
			CircuitFailures:     cfg.AIProviderCircuitFailures,
			CircuitOpenFor:      time.Duration(cfg.AIProviderCircuitOpenMS) * time.Millisecond,
			DefaultOutputTokens: cfg.AIDefaultOutputTokens,
			SLO: executor.AISLOConfig{
				Realtime: executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLORealtimeQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLORealtimeTimeoutMS) * time.Millisecond},
				Standard: executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLOStandardQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLOStandardTimeoutMS) * time.Millisecond},
				Batch:    executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLOBatchQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLOBatchTimeoutMS) * time.Millisecond},
			},
			FairScheduling: executor.AIFairSchedulingConfig{
				Enabled:                 cfg.AITenantFairScheduling,
				MaxConsecutivePerTenant: cfg.AITenantMaxConsecutive,
				StarvationAfter:         time.Duration(cfg.AITenantStarvationMS) * time.Millisecond,
			},
			DistributedCoordinator: distributedCoordinator,
			TenantCoordinator:      tenantCoordinator,
			TenantQuota: executor.AITenantDistributedQuotaConfig{
				Enabled:      cfg.AITenantQuotaEnabled,
				DefaultLimit: cfg.AITenantQuotaLimit,
				Overrides:    executor.ParseAITenantConcurrencyOverrides(cfg.AITenantQuotaOverrides),
			},
			DistributedLeaseTTL: time.Duration(cfg.AIDistributedLeaseMS) * time.Millisecond,
			LoadShedding: executor.AILoadSheddingConfig{
				Enabled:               cfg.AIAdaptiveLoadShedding,
				DegradedQueueDepth:    cfg.AIPressureQueueWarn,
				OverloadedQueueDepth:  cfg.AIPressureQueueHigh,
				DegradedFailureRate:   cfg.AIPressureFailWarn,
				OverloadedFailureRate: cfg.AIPressureFailHigh,
				DegradedLatency:       time.Duration(cfg.AIPressureLatencyWarnMS) * time.Millisecond,
				OverloadedLatency:     time.Duration(cfg.AIPressureLatencyHighMS) * time.Millisecond,
			},
			Hedge: executor.AIHedgeConfig{
				Enabled:         cfg.AIHedgeEnabled,
				Delay:           time.Duration(cfg.AIHedgeDelayMS) * time.Millisecond,
				CancelWait:      time.Duration(cfg.AIHedgeCancelWaitMS) * time.Millisecond,
				MaxExtraCostUSD: cfg.AIHedgeMaxExtraCostUSD,
			},
		},
		executor.AIProviderConfig{
			Name: cfg.AIProvider, Kind: cfg.AIProviderKind, Endpoint: cfg.AIEndpoint, BearerToken: cfg.AIBearerToken,
			Model: cfg.AIModel, Capabilities: executor.ParseAICapabilities(cfg.AICapabilities),
			InputCostPer1M: cfg.AIInputCostPer1M, OutputCostPer1M: cfg.AIOutputCostPer1M, MaxConcurrency: cfg.AIMaxConcurrency, GlobalMaxConcurrency: cfg.AIGlobalConcurrencyLimit,
		},
		executor.AIProviderConfig{
			Name: cfg.AIFallbackProvider, Kind: cfg.AIFallbackKind, Endpoint: cfg.AIFallbackEndpoint, BearerToken: cfg.AIFallbackToken,
			Model: cfg.AIFallbackModel, Capabilities: executor.ParseAICapabilities(cfg.AIFallbackCaps),
			InputCostPer1M: cfg.AIFallbackInputCostPer1M, OutputCostPer1M: cfg.AIFallbackOutputCostPer1M, MaxConcurrency: cfg.AIFallbackMaxConcurrency, GlobalMaxConcurrency: cfg.AIFallbackGlobalLimit,
		},
	)
	rpc.RegisterExecutorServiceServer(server, executor.NewGRPCServer(taskExecutor))
	healthServer := health.NewServer()
	healthServer.SetServingStatus(rpc.ExecutorServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	metricsServer := &http.Server{
		Addr:              cfg.ExecutorMetricsAddr,
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("executor-worker grpc listening on %s", cfg.ExecutorGRPCListen)
		if err := server.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	go func() {
		log.Printf("executor-worker metrics listening on %s", cfg.ExecutorMetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	appmetrics.ProcessDraining.Set(1)
	log.Printf("executor-worker draining: health set to NOT_SERVING")
	healthServer.SetServingStatus(rpc.ExecutorServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		appmetrics.DrainOutcomes.WithLabelValues("executor", "graceful").Inc()
		log.Printf("executor-worker drain completed")
	case <-time.After(time.Duration(cfg.DrainTimeoutMS) * time.Millisecond):
		appmetrics.DrainOutcomes.WithLabelValues("executor", "timeout").Inc()
		log.Printf("executor-worker drain timeout; forcing active RPC cancellation")
		server.Stop()
		<-done
	}
	metricsShutdownCtx, metricsCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		log.Printf("metrics shutdown: %v", err)
	}
	metricsCancel()
}
