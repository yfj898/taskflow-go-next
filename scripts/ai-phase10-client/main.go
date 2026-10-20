package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: ai-phase10-client <executor-a> <executor-b>")
		os.Exit(2)
	}
	runQuota(os.Args[1], os.Args[2])
}

func runQuota(targetA, targetB string) {
	a, err := executor.NewGRPCExecutor(targetA, 6*time.Second)
	if err != nil {
		panic(err)
	}
	defer a.Close()
	b, err := executor.NewGRPCExecutor(targetB, 6*time.Second)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	a.ConfigureTaskTimeout(executor.AITaskType, 6*time.Second)
	b.ConfigureTaskTimeout(executor.AITaskType, 6*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holderDone := make(chan error, 1)
	go func() {
		result, callErr := execute(a, ctx, 10001, "quota-holder", "tenant-a")
		if callErr == nil && !strings.Contains(result, "slow-ai:quota-holder") {
			callErr = fmt.Errorf("holder result=%q", result)
		}
		holderDone <- callErr
	}()

	// The slow provider remains active long enough for executor A to own the
	// tenant-scoped Redis lease before executor B tries the same tenant.
	time.Sleep(200 * time.Millisecond)
	started := time.Now()
	_, err = execute(b, ctx, 10002, "same-tenant-overflow", "tenant-a")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tenant") || !strings.Contains(strings.ToLower(err.Error()), "concurrency") {
		panic(fmt.Errorf("expected distributed tenant quota error, got %v", err))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		panic(fmt.Errorf("tenant quota rejection took too long: %s", elapsed))
	}

	if err := <-holderDone; err != nil {
		panic(fmt.Errorf("holder failed: %w", err))
	}

	result, err := execute(b, ctx, 10003, "different-tenant", "tenant-b")
	if err != nil {
		panic(fmt.Errorf("different tenant should remain admissible: %w", err))
	}
	if !strings.Contains(result, "slow-ai:different-tenant") {
		panic(fmt.Errorf("different tenant result=%q", result))
	}
	fmt.Println("phase10 distributed tenant quota: ok")
}

func execute(client executor.Executor, ctx context.Context, id uint64, prompt, tenantID string) (string, error) {
	payload := fmt.Sprintf(`{"prompt":%q,"tenant_id":%q,"model":"slow-v1","slo_class":"standard"}`, prompt, tenantID)
	return client.Execute(ctx, &domain.Task{
		ID:      id,
		Name:    "phase10-tenant-quota",
		Type:    executor.AITaskType,
		Payload: payload,
	})
}
