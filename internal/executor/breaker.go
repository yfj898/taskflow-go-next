package executor

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("executor circuit breaker open")

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type CircuitBreaker struct {
	mu          sync.Mutex
	state       breakerState
	failures    int
	threshold   int
	openFor     time.Duration
	openedAt    time.Time
	halfOpenRun bool
	now         func() time.Time
}

func NewCircuitBreaker(threshold int, openFor time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if openFor <= 0 {
		openFor = 5 * time.Second
	}
	return &CircuitBreaker{
		threshold: threshold,
		openFor:   openFor,
		now:       time.Now,
	}
}

func (b *CircuitBreaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return nil
	case breakerOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return ErrCircuitOpen
		}
		b.state = breakerHalfOpen
		b.halfOpenRun = true
		return nil
	case breakerHalfOpen:
		if b.halfOpenRun {
			return ErrCircuitOpen
		}
		b.halfOpenRun = true
		return nil
	default:
		return ErrCircuitOpen
	}
}

func (b *CircuitBreaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = breakerClosed
	b.failures = 0
	b.halfOpenRun = false
}

func (b *CircuitBreaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == breakerHalfOpen {
		b.openLocked()
		return
	}

	b.failures++
	if b.failures >= b.threshold {
		b.openLocked()
	}
}

func (b *CircuitBreaker) openLocked() {
	b.state = breakerOpen
	b.openedAt = b.now()
	b.halfOpenRun = false
}

func (b *CircuitBreaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return "closed"
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}
