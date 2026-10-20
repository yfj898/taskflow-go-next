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
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ai-phase7-client shared <executor-a> <executor-b> | call <executor> <prompt> <model> <slo> <expected> | shed <executor>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "shared":
		if len(os.Args) != 4 {
			os.Exit(2)
		}
		runShared(os.Args[2], os.Args[3])
	case "call":
		if len(os.Args) != 7 {
			os.Exit(2)
		}
		runCall(os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6])
	case "shed":
		if len(os.Args) != 3 {
			os.Exit(2)
		}
		runShed(os.Args[2])
	default:
		os.Exit(2)
	}
}

func runShared(targetA, targetB string) {
	a, err := executor.NewGRPCExecutor(targetA, 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer a.Close()
	b, err := executor.NewGRPCExecutor(targetB, 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	a.ConfigureTaskTimeout(executor.AITaskType, 5*time.Second)
	b.ConfigureTaskTimeout(executor.AITaskType, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	holderDone := make(chan error, 1)
	go func() {
		result, err := a.Execute(ctx, &domain.Task{
			ID:      7001,
			Name:    "phase7-holder",
			Type:    executor.AITaskType,
			Payload: `{"prompt":"holder","model":"slow-v1","slo_class":"standard"}`,
		})
		if err == nil && !strings.Contains(result, "slow-ai:holder") {
			err = fmt.Errorf("holder result=%q", result)
		}
		holderDone <- err
	}()

	// The slow mock runs for 700ms. This gives executor A enough time to own the
	// Redis lease before executor B attempts the same provider/model.
	time.Sleep(150 * time.Millisecond)
	result, err := b.Execute(ctx, &domain.Task{
		ID:      7002,
		Name:    "phase7-overflow",
		Type:    executor.AITaskType,
		Payload: `{"prompt":"overflow","slo_class":"realtime","routing_strategy":"cost"}`,
	})
	if err != nil {
		panic(fmt.Errorf("overflow execution: %w", err))
	}
	if !strings.Contains(result, "mock-ai:overflow") {
		panic(fmt.Errorf("overflow did not use fast fallback: %q", result))
	}
	if err := <-holderDone; err != nil {
		panic(fmt.Errorf("holder execution: %w", err))
	}
	fmt.Println("phase7 distributed concurrency: ok")
}

func runCall(target, prompt, model, slo, expected string) {
	client, err := executor.NewGRPCExecutor(target, 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	client.ConfigureTaskTimeout(executor.AITaskType, 5*time.Second)
	payload := fmt.Sprintf(`{"prompt":%q,"model":%q,"slo_class":%q}`, prompt, model, slo)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Execute(ctx, &domain.Task{ID: 7003, Name: "phase7-call", Type: executor.AITaskType, Payload: payload})
	if err != nil {
		panic(err)
	}
	if !strings.Contains(result, expected) {
		panic(fmt.Errorf("result=%q, expected substring %q", result, expected))
	}
	fmt.Println(result)
}

func runShed(target string) {
	client, err := executor.NewGRPCExecutor(target, 6*time.Second)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	client.ConfigureTaskTimeout(executor.AITaskType, 6*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	call := func(id uint64, prompt, slo string) error {
		_, err := client.Execute(ctx, &domain.Task{
			ID:      id,
			Name:    "phase7-shed",
			Type:    executor.AITaskType,
			Payload: fmt.Sprintf(`{"prompt":%q,"model":"slow-v1","slo_class":%q}`, prompt, slo),
		})
		return err
	}

	done := make(chan error, 3)
	go func() { done <- call(7101, "shed-holder", executor.AISLOStandard) }()
	time.Sleep(120 * time.Millisecond)
	go func() { done <- call(7102, "shed-queued-1", executor.AISLOBatch) }()
	go func() { done <- call(7103, "shed-queued-2", executor.AISLOBatch) }()
	// Give both concurrent gRPC calls enough time to cross transport setup and
	// become visible in the executor's local gate before sampling pressure with
	// the final batch request. The old 120ms interval was timing-sensitive on a
	// cold Docker network and could exercise ordinary queue backpressure instead
	// of the intended overloaded load-shedding branch.
	time.Sleep(300 * time.Millisecond)
	err = call(7104, "shed-rejected", executor.AISLOBatch)
	if err == nil || (!strings.Contains(err.Error(), "shed") && !strings.Contains(err.Error(), "overloaded")) {
		panic(fmt.Errorf("expected overloaded load shedding error, got %v", err))
	}
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			panic(fmt.Errorf("background load test call failed: %w", err))
		}
	}
	fmt.Println("phase7 adaptive load shedding: ok")
}
