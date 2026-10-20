package executor

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAndHalfOpens(t *testing.T) {
	b := NewCircuitBreaker(2, time.Second)
	now := time.Unix(100, 0)
	b.now = func() time.Time { return now }

	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Failure()
	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Failure()

	if !errors.Is(b.Allow(), ErrCircuitOpen) {
		t.Fatal("breaker should be open")
	}

	now = now.Add(2 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatalf("half-open probe should be allowed: %v", err)
	}
	if !errors.Is(b.Allow(), ErrCircuitOpen) {
		t.Fatal("only one half-open probe should be allowed")
	}

	b.Success()
	if got := b.State(); got != "closed" {
		t.Fatalf("state = %q, want closed", got)
	}
}

func TestCircuitBreakerHalfOpenFailureReopens(t *testing.T) {
	b := NewCircuitBreaker(1, time.Second)
	now := time.Unix(200, 0)
	b.now = func() time.Time { return now }

	b.Failure()
	now = now.Add(2 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatal(err)
	}
	b.Failure()
	if !errors.Is(b.Allow(), ErrCircuitOpen) {
		t.Fatal("failed half-open probe should reopen breaker")
	}
}
