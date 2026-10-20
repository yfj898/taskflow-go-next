package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type UpdateAIBudgetInput struct {
	LimitUSD    float64 `json:"limit_usd"`
	LimitTokens int64   `json:"limit_tokens"`
}

type AIBudgetAccountView struct {
	ID              uint64   `json:"id"`
	Scope           string   `json:"scope"`
	ScopeKey        string   `json:"scope_key"`
	LimitUSD        float64  `json:"limit_usd"`
	LimitTokens     int64    `json:"limit_tokens"`
	SpentUSD        float64  `json:"spent_usd"`
	SpentTokens     int64    `json:"spent_tokens"`
	ReservedUSD     float64  `json:"reserved_usd"`
	ReservedTokens  int64    `json:"reserved_tokens"`
	RemainingUSD    *float64 `json:"remaining_usd,omitempty"`
	RemainingTokens *int64   `json:"remaining_tokens,omitempty"`
	Version         uint64   `json:"version"`
}

type AIBudgetManager struct {
	repo              *repository.TaskRepository
	policy            *AIRoutingPolicyManager
	defaultReserveUSD float64
}

func NewAIBudgetManager(repo *repository.TaskRepository, policy *AIRoutingPolicyManager, defaultReserveUSD float64) *AIBudgetManager {
	if defaultReserveUSD < 0 || math.IsNaN(defaultReserveUSD) || math.IsInf(defaultReserveUSD, 0) {
		defaultReserveUSD = 0
	}
	return &AIBudgetManager{repo: repo, policy: policy, defaultReserveUSD: defaultReserveUSD}
}

func (m *AIBudgetManager) ReserveForTask(ctx context.Context, task *domain.Task) (*domain.AIBudgetReservation, error) {
	if task == nil || !strings.EqualFold(strings.TrimSpace(task.Type), executor.AITaskType) {
		return nil, nil
	}
	hints, err := executor.ParseAIBudgetHints(task)
	if err != nil {
		return nil, err
	}
	reserveUSD := m.defaultReserveUSD
	if hints.MaxCostUSD > 0 {
		reserveUSD = hints.MaxCostUSD
	}
	defaultOutputTokens := 256
	if m.policy != nil {
		if current := m.policy.Current(); current.DefaultOutputTokens > 0 {
			defaultOutputTokens = current.DefaultOutputTokens
		}
	}
	reserveTokens := executor.EstimateAITokenReservation(hints.Prompt, defaultOutputTokens)
	reservation, err := m.repo.ReserveAIBudget(ctx, task.ID, task.RetryCount, hints.TenantID, reserveUSD, reserveTokens)
	if err != nil {
		var exceeded *repository.AIBudgetExceededError
		if errors.As(err, &exceeded) {
			appmetrics.AIBudgetAdmissions.WithLabelValues("denied", exceeded.Kind).Inc()
			return nil, executor.Permanent(err)
		}
		return nil, executor.Retryable(fmt.Errorf("reserve AI budget: %w", err))
	}
	if reservation == nil {
		appmetrics.AIBudgetAdmissions.WithLabelValues("allowed", "unmetered").Inc()
		return nil, nil
	}
	appmetrics.AIBudgetAdmissions.WithLabelValues("allowed", "reserved").Inc()
	return reservation, nil
}

func (m *AIBudgetManager) UpdateAccount(ctx context.Context, scope, key string, in UpdateAIBudgetInput) (AIBudgetAccountView, error) {
	account, err := m.repo.UpsertAIBudgetAccount(ctx, scope, key, in.LimitUSD, in.LimitTokens)
	if err != nil {
		return AIBudgetAccountView{}, err
	}
	return budgetAccountView(*account), nil
}

func (m *AIBudgetManager) GetAccount(ctx context.Context, scope, key string) (AIBudgetAccountView, error) {
	account, err := m.repo.GetAIBudgetAccount(ctx, scope, key)
	if err != nil {
		return AIBudgetAccountView{}, err
	}
	return budgetAccountView(*account), nil
}

func (m *AIBudgetManager) ListAccounts(ctx context.Context) ([]AIBudgetAccountView, error) {
	accounts, err := m.repo.ListAIBudgetAccounts(ctx)
	if err != nil {
		return nil, err
	}
	views := make([]AIBudgetAccountView, 0, len(accounts))
	for _, account := range accounts {
		views = append(views, budgetAccountView(account))
	}
	return views, nil
}

func budgetAccountView(account domain.AIBudgetAccount) AIBudgetAccountView {
	view := AIBudgetAccountView{
		ID: account.ID, Scope: account.Scope, ScopeKey: account.ScopeKey,
		LimitUSD: account.LimitUSD, LimitTokens: account.LimitTokens,
		SpentUSD: account.SpentUSD, SpentTokens: account.SpentTokens,
		ReservedUSD: account.ReservedUSD, ReservedTokens: account.ReservedTokens,
		Version: account.Version,
	}
	if account.LimitUSD > 0 {
		remaining := account.LimitUSD - account.SpentUSD - account.ReservedUSD
		if remaining < 0 {
			remaining = 0
		}
		view.RemainingUSD = &remaining
	}
	if account.LimitTokens > 0 {
		remaining := account.LimitTokens - account.SpentTokens - account.ReservedTokens
		if remaining < 0 {
			remaining = 0
		}
		view.RemainingTokens = &remaining
	}
	return view
}
