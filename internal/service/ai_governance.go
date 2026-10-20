package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
	"github.com/yfj898/taskflow-go/internal/repository"
)

type UpdateAIRoutingPolicyInput struct {
	Strategy            string `json:"strategy"`
	DefaultOutputTokens int    `json:"default_output_tokens"`
}

type UpdateAITenantQuotaPolicyInput struct {
	Enabled      bool   `json:"enabled"`
	DefaultLimit int    `json:"default_limit"`
	Overrides    string `json:"overrides"`
}

type AIRouteAuditView struct {
	ID        uint64          `json:"id"`
	TaskID    uint64          `json:"task_id"`
	Attempt   int             `json:"attempt"`
	Decision  json.RawMessage `json:"decision"`
	CreatedAt time.Time       `json:"created_at"`
}

type AIRoutingPolicyManager struct {
	repo *repository.TaskRepository

	mu      sync.RWMutex
	current domain.AIRoutingPolicy
}

func NewAIRoutingPolicyManager(repo *repository.TaskRepository, strategy string, defaultOutputTokens int) *AIRoutingPolicyManager {
	strategy = executor.NormalizeAIRoutingStrategy(strategy)
	if strategy == "" {
		strategy = executor.AIRoutingBalanced
	}
	if defaultOutputTokens <= 0 {
		defaultOutputTokens = 256
	}
	return &AIRoutingPolicyManager{
		repo: repo,
		current: domain.AIRoutingPolicy{
			ID:                  1,
			Version:             1,
			Strategy:            strategy,
			DefaultOutputTokens: defaultOutputTokens,
		},
	}
}

func (m *AIRoutingPolicyManager) Init(ctx context.Context) error {
	policy, err := m.repo.GetAIRoutingPolicy(ctx)
	if errors.Is(err, repository.ErrAIRoutingPolicyNotFound) {
		m.mu.RLock()
		initial := m.current
		m.mu.RUnlock()
		initial.UpdatedAt = time.Now()
		if err := m.repo.UpsertAIRoutingPolicy(ctx, &initial); err != nil {
			return err
		}
		m.set(initial)
		return nil
	}
	if err != nil {
		return err
	}
	if executor.NormalizeAIRoutingStrategy(policy.Strategy) == "" || policy.DefaultOutputTokens <= 0 {
		return fmt.Errorf("invalid persisted AI routing policy version=%d", policy.Version)
	}
	if policy.TenantQuotaManaged {
		canonical, overrides, err := executor.NormalizeAITenantConcurrencyOverrides(policy.TenantQuotaOverrides)
		if err != nil || policy.TenantQuotaDefaultLimit < 0 || (policy.TenantQuotaEnabled && policy.TenantQuotaDefaultLimit == 0 && len(overrides) == 0) {
			return fmt.Errorf("invalid persisted AI tenant quota policy version=%d", policy.Version)
		}
		policy.TenantQuotaOverrides = canonical
	}
	m.set(*policy)
	return nil
}

func (m *AIRoutingPolicyManager) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = m.Refresh(ctx)
			}
		}
	}()
}

func (m *AIRoutingPolicyManager) Refresh(ctx context.Context) error {
	policy, err := m.repo.GetAIRoutingPolicy(ctx)
	if err != nil {
		return err
	}
	m.mu.RLock()
	currentVersion := m.current.Version
	m.mu.RUnlock()
	if policy.Version > currentVersion {
		m.set(*policy)
	}
	return nil
}

func (m *AIRoutingPolicyManager) CurrentAIRoutingPolicy() executor.AIRoutingPolicySnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return executor.AIRoutingPolicySnapshot{
		Version:                 m.current.Version,
		Strategy:                m.current.Strategy,
		DefaultOutputTokens:     m.current.DefaultOutputTokens,
		TenantQuotaManaged:      m.current.TenantQuotaManaged,
		TenantQuotaEnabled:      m.current.TenantQuotaEnabled,
		TenantQuotaDefaultLimit: m.current.TenantQuotaDefaultLimit,
		TenantQuotaOverrides:    m.current.TenantQuotaOverrides,
	}
}

func (m *AIRoutingPolicyManager) Current() domain.AIRoutingPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

func (m *AIRoutingPolicyManager) Update(ctx context.Context, in UpdateAIRoutingPolicyInput) (domain.AIRoutingPolicy, error) {
	strategy := executor.NormalizeAIRoutingStrategy(in.Strategy)
	if strategy == "" {
		return domain.AIRoutingPolicy{}, fmt.Errorf("unsupported AI routing strategy %q", in.Strategy)
	}
	if in.DefaultOutputTokens <= 0 {
		return domain.AIRoutingPolicy{}, errors.New("default_output_tokens must be positive")
	}

	policy, err := m.repo.UpdateAIRoutingPolicy(ctx, strategy, in.DefaultOutputTokens)
	if err != nil {
		return domain.AIRoutingPolicy{}, err
	}
	m.set(*policy)
	return *policy, nil
}

func (m *AIRoutingPolicyManager) UpdateTenantQuota(ctx context.Context, in UpdateAITenantQuotaPolicyInput) (domain.AIRoutingPolicy, error) {
	if in.DefaultLimit < 0 {
		return domain.AIRoutingPolicy{}, errors.New("default_limit must be non-negative")
	}
	canonical, overrides, err := executor.NormalizeAITenantConcurrencyOverrides(in.Overrides)
	if err != nil {
		return domain.AIRoutingPolicy{}, err
	}
	if in.Enabled && in.DefaultLimit == 0 && len(overrides) == 0 {
		return domain.AIRoutingPolicy{}, errors.New("enabled tenant quota requires a positive default_limit or override")
	}
	policy, err := m.repo.UpdateAITenantQuotaPolicy(ctx, in.Enabled, in.DefaultLimit, canonical)
	if err != nil {
		return domain.AIRoutingPolicy{}, err
	}
	m.set(*policy)
	return *policy, nil
}

func (m *AIRoutingPolicyManager) ResetTenantQuota(ctx context.Context) (domain.AIRoutingPolicy, error) {
	policy, err := m.repo.ResetAITenantQuotaPolicy(ctx)
	if err != nil {
		return domain.AIRoutingPolicy{}, err
	}
	m.set(*policy)
	return *policy, nil
}

func (m *AIRoutingPolicyManager) set(policy domain.AIRoutingPolicy) {
	m.mu.Lock()
	m.current = policy
	m.mu.Unlock()
}

type AIGovernanceService struct {
	repo   *repository.TaskRepository
	policy *AIRoutingPolicyManager
	budget *AIBudgetManager
}

func NewAIGovernanceService(repo *repository.TaskRepository, policy *AIRoutingPolicyManager) *AIGovernanceService {
	return &AIGovernanceService{repo: repo, policy: policy}
}

func (s *AIGovernanceService) ConfigureBudgetManager(budget *AIBudgetManager) {
	s.budget = budget
}

func (s *AIGovernanceService) UpdateBudget(ctx context.Context, scope, key string, in UpdateAIBudgetInput) (AIBudgetAccountView, error) {
	if s.budget == nil {
		return AIBudgetAccountView{}, errors.New("AI budget governance is not configured")
	}
	return s.budget.UpdateAccount(ctx, scope, key, in)
}

func (s *AIGovernanceService) GetBudget(ctx context.Context, scope, key string) (AIBudgetAccountView, error) {
	if s.budget == nil {
		return AIBudgetAccountView{}, errors.New("AI budget governance is not configured")
	}
	return s.budget.GetAccount(ctx, scope, key)
}

func (s *AIGovernanceService) ListBudgets(ctx context.Context) ([]AIBudgetAccountView, error) {
	if s.budget == nil {
		return nil, errors.New("AI budget governance is not configured")
	}
	return s.budget.ListAccounts(ctx)
}

func (s *AIGovernanceService) Policy() domain.AIRoutingPolicy {
	return s.policy.Current()
}

func (s *AIGovernanceService) UpdatePolicy(ctx context.Context, in UpdateAIRoutingPolicyInput) (domain.AIRoutingPolicy, error) {
	return s.policy.Update(ctx, in)
}

func (s *AIGovernanceService) UpdateTenantQuotaPolicy(ctx context.Context, in UpdateAITenantQuotaPolicyInput) (domain.AIRoutingPolicy, error) {
	return s.policy.UpdateTenantQuota(ctx, in)
}

func (s *AIGovernanceService) ResetTenantQuotaPolicy(ctx context.Context) (domain.AIRoutingPolicy, error) {
	return s.policy.ResetTenantQuota(ctx)
}

func (s *AIGovernanceService) ListAudits(ctx context.Context, taskID uint64) ([]AIRouteAuditView, error) {
	if _, err := s.repo.Get(ctx, taskID); err != nil {
		return nil, err
	}
	audits, err := s.repo.ListAIRouteAudits(ctx, taskID)
	if err != nil {
		return nil, err
	}
	views := make([]AIRouteAuditView, 0, len(audits))
	for _, audit := range audits {
		views = append(views, AIRouteAuditView{
			ID:        audit.ID,
			TaskID:    audit.TaskID,
			Attempt:   audit.Attempt,
			Decision:  json.RawMessage(audit.MetadataJSON),
			CreatedAt: audit.CreatedAt,
		})
	}
	return views, nil
}

func (s *AIGovernanceService) ReplayAudit(ctx context.Context, taskID, auditID uint64, strategy string) (executor.AIRouteReplay, error) {
	audit, err := s.repo.GetAIRouteAudit(ctx, taskID, auditID)
	if err != nil {
		return executor.AIRouteReplay{}, err
	}
	return executor.ReplayAIRouteDecision(audit.MetadataJSON, strategy)
}
