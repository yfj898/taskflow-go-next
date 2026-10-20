package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var ErrInvalidTask = errors.New("name and type are required")
var ErrReservedTaskType = errors.New("verify_change requires the authenticated verification-runs API")

type CreateTaskInput struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type TaskService struct {
	repo *repository.TaskRepository
}

func NewTaskService(repo *repository.TaskRepository, q queue.TaskQueue) *TaskService {
	_ = q
	return &TaskService{repo: repo}
}

func (s *TaskService) Create(ctx context.Context, in CreateTaskInput) (*domain.Task, error) {
	if strings.EqualFold(strings.TrimSpace(in.Type), domain.VerificationTaskType) {
		return nil, ErrReservedTaskType
	}
	if strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.Type) == "" {
		return nil, ErrInvalidTask
	}

	task := &domain.Task{
		Name: in.Name, Type: in.Type, Payload: in.Payload,
		Status: domain.TaskPending, NextRunAt: time.Now(), EnqueueGeneration: 1,
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	task.TraceParent = carrier.Get("traceparent")
	task.TraceState = carrier.Get("tracestate")
	if err := s.repo.CreateWithOutbox(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

func (s *TaskService) Get(ctx context.Context, id uint64) (*domain.Task, error) {
	task, err := s.repo.Get(ctx, id)
	if err == nil && task.Type == domain.VerificationTaskType {
		return nil, repository.ErrTaskNotFound
	}
	return task, err
}

func (s *TaskService) List(ctx context.Context, limit, offset int) ([]domain.Task, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListPublicTasks(ctx, limit, offset)
}
