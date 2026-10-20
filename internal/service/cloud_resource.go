package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/repository"
)

var (
	ErrInvalidCloudResource  = errors.New("invalid cloud resource")
	ErrInvalidCloudQuota     = errors.New("invalid cloud tenant quota")
	ErrCloudProviderDisabled = errors.New("cloud resource provider is disabled")
	ErrCloudReconcileBusy    = errors.New("cloud resource is already being reconciled")
	ErrIdempotencyConflict   = errors.New("idempotency key conflicts with another operation")
)

var kubernetesNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type CreateCloudResourceInput struct {
	TenantID string          `json:"tenant_id"`
	Name     string          `json:"name"`
	Kind     string          `json:"kind"`
	Provider string          `json:"provider"`
	Spec     json.RawMessage `json:"spec"`
}

type ResizeCloudResourceInput struct {
	Spec json.RawMessage `json:"spec"`
}

type SetCloudTenantQuotaInput struct {
	CPUMilliLimit  int64 `json:"cpu_milli_limit"`
	MemoryMiBLimit int64 `json:"memory_mib_limit"`
	ReplicaLimit   int64 `json:"replica_limit"`
}

type CloudTenantQuotaView struct {
	TenantID           string `json:"tenant_id"`
	CPUMilliLimit      int64  `json:"cpu_milli_limit"`
	MemoryMiBLimit     int64  `json:"memory_mib_limit"`
	ReplicaLimit       int64  `json:"replica_limit"`
	CPUMilliUsed       int64  `json:"cpu_milli_used"`
	MemoryMiBUsed      int64  `json:"memory_mib_used"`
	ReplicaUsed        int64  `json:"replica_used"`
	CPUMilliAvailable  int64  `json:"cpu_milli_available"`
	MemoryMiBAvailable int64  `json:"memory_mib_available"`
	ReplicaAvailable   int64  `json:"replica_available"`
}

type CloudMutationResult struct {
	Resource *domain.CloudResource `json:"resource"`
	Order    *domain.CloudOrder    `json:"order"`
}

type CloudResourceService struct {
	repo           *repository.CloudResourceRepository
	provider       cloudcontrol.ResourceProvider
	reconcileLease time.Duration
	driftInterval  time.Duration
}

func NewCloudResourceService(repo *repository.CloudResourceRepository, provider cloudcontrol.ResourceProvider) *CloudResourceService {
	return &CloudResourceService{
		repo:           repo,
		provider:       provider,
		reconcileLease: 30 * time.Second,
		driftInterval:  60 * time.Second,
	}
}

func (s *CloudResourceService) Create(ctx context.Context, in CreateCloudResourceInput, idempotencyKey string) (*CloudMutationResult, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentResult(ctx, idempotencyKey, 0, domain.CloudOrderCreate, in.TenantID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.Resource.TenantID != strings.TrimSpace(in.TenantID) || existing.Resource.Name != strings.TrimSpace(in.Name) {
			return nil, ErrIdempotencyConflict
		}
		return existing, nil
	}

	tenant := strings.TrimSpace(in.TenantID)
	name := strings.TrimSpace(in.Name)
	kind := strings.ToLower(strings.TrimSpace(in.Kind))
	provider := strings.ToLower(strings.TrimSpace(in.Provider))
	if kind == "" {
		kind = "deployment"
	}
	if provider == "" {
		provider = "kubernetes"
	}
	if tenant == "" || !validKubernetesName(name) || kind != "deployment" || provider != "kubernetes" {
		return nil, ErrInvalidCloudResource
	}
	spec, demand, err := normalizeDeploymentSpec(in.Spec)
	if err != nil {
		return nil, err
	}
	resource := &domain.CloudResource{
		TenantID:          tenant,
		Name:              name,
		Kind:              kind,
		Provider:          provider,
		OrchestrationMode: domain.CloudOrchestrationDirect,
		DesiredSpec:       spec,
		DesiredState:      domain.CloudResourceDesiredActive,
		ActualState:       domain.CloudResourcePending,
		Generation:        1,
	}
	order := &domain.CloudOrder{
		TenantID:       tenant,
		Type:           domain.CloudOrderCreate,
		Status:         domain.CloudOrderPending,
		IdempotencyKey: idempotencyKey,
		RequestedSpec:  spec,
	}
	if err := s.repo.CreateResourceWithOrder(ctx, resource, order, demand); err != nil {
		if existing, lookupErr := s.idempotentResult(ctx, idempotencyKey, 0, domain.CloudOrderCreate, in.TenantID); lookupErr == nil && existing != nil {
			// A concurrent caller can win the globally unique idempotency
			// key with different tenant/name. The retry path must enforce
			// the same ownership/intent check as the fast replay path.
			if existing.Resource.TenantID != tenant || existing.Resource.Name != name {
				return nil, ErrIdempotencyConflict
			}
			return existing, nil
		}
		return nil, err
	}
	return &CloudMutationResult{Resource: resource, Order: order}, nil
}

func (s *CloudResourceService) Resize(ctx context.Context, id uint64, in ResizeCloudResourceInput, idempotencyKey string) (*CloudMutationResult, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentResult(ctx, idempotencyKey, id, domain.CloudOrderResize, ""); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	spec, demand, err := normalizeDeploymentSpec(in.Spec)
	if err != nil {
		return nil, err
	}
	current, err := s.repo.GetResource(ctx, id)
	if err != nil {
		return nil, err
	}
	order := &domain.CloudOrder{
		TenantID:       current.TenantID,
		Type:           domain.CloudOrderResize,
		Status:         domain.CloudOrderPending,
		IdempotencyKey: idempotencyKey,
	}
	resource, err := s.repo.ResizeResourceWithOrder(ctx, id, spec, demand, order)
	if err != nil {
		if existing, lookupErr := s.idempotentResult(ctx, idempotencyKey, id, domain.CloudOrderResize, ""); lookupErr == nil && existing != nil {
			return existing, nil
		}
		return nil, err
	}
	return &CloudMutationResult{Resource: resource, Order: order}, nil
}

func (s *CloudResourceService) Delete(ctx context.Context, id uint64, idempotencyKey string) (*CloudMutationResult, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentResult(ctx, idempotencyKey, id, domain.CloudOrderDelete, ""); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	current, err := s.repo.GetResource(ctx, id)
	if err != nil {
		return nil, err
	}
	order := &domain.CloudOrder{
		TenantID:       current.TenantID,
		Type:           domain.CloudOrderDelete,
		Status:         domain.CloudOrderPending,
		IdempotencyKey: idempotencyKey,
	}
	resource, err := s.repo.DeleteResourceWithOrder(ctx, id, order)
	if err != nil {
		if existing, lookupErr := s.idempotentResult(ctx, idempotencyKey, id, domain.CloudOrderDelete, ""); lookupErr == nil && existing != nil {
			return existing, nil
		}
		return nil, err
	}
	return &CloudMutationResult{Resource: resource, Order: order}, nil
}

func (s *CloudResourceService) Get(ctx context.Context, id uint64) (*domain.CloudResource, error) {
	return s.repo.GetResource(ctx, id)
}

func (s *CloudResourceService) List(ctx context.Context, tenant string, limit, offset int) ([]domain.CloudResource, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListResources(ctx, strings.TrimSpace(tenant), limit, offset)
}

func (s *CloudResourceService) ListOrders(ctx context.Context, resourceID uint64, limit, offset int) ([]domain.CloudOrder, error) {
	if _, err := s.repo.GetResource(ctx, resourceID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListOrders(ctx, resourceID, limit, offset)
}

func (s *CloudResourceService) SetTenantQuota(ctx context.Context, tenantID string, in SetCloudTenantQuotaInput) (*CloudTenantQuotaView, error) {
	tenantID = strings.TrimSpace(tenantID)
	limits := cloudcontrol.QuotaDemand{
		CPUMilli:  in.CPUMilliLimit,
		MemoryMiB: in.MemoryMiBLimit,
		Replicas:  in.ReplicaLimit,
	}
	if tenantID == "" || !limits.NonNegative() {
		return nil, ErrInvalidCloudQuota
	}
	quota, err := s.repo.SetTenantQuota(ctx, tenantID, limits)
	if err != nil {
		return nil, err
	}
	return cloudTenantQuotaView(quota), nil
}

func (s *CloudResourceService) GetTenantQuota(ctx context.Context, tenantID string) (*CloudTenantQuotaView, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, ErrInvalidCloudQuota
	}
	quota, err := s.repo.GetTenantQuota(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return cloudTenantQuotaView(quota), nil
}

func (s *CloudResourceService) ReconcileOne(ctx context.Context, id uint64) error {
	if s.provider == nil {
		return ErrCloudProviderDisabled
	}
	resource, err := s.repo.GetResource(ctx, id)
	if err != nil {
		return err
	}
	settledQuota := cloudcontrol.QuotaDemand{}
	if resource.DesiredState == domain.CloudResourceDesiredActive {
		_, demand, parseErr := normalizeDeploymentSpec(json.RawMessage(resource.DesiredSpec))
		if parseErr != nil {
			_, _ = s.repo.RecordReconcileFailure(ctx, resource.ID, resource.Generation, boundedCloudError(parseErr))
			return parseErr
		}
		settledQuota = demand
	}
	claimed, err := s.repo.ClaimReconcile(ctx, resource.ID, resource.Generation, time.Now().Add(s.reconcileLease))
	if err != nil {
		return err
	}
	if !claimed {
		return ErrCloudReconcileBusy
	}
	if err := s.repo.MarkOrderRunning(ctx, resource.ID, resource.Generation); err != nil {
		_, _ = s.repo.RecordReconcileFailure(ctx, resource.ID, resource.Generation, err.Error())
		return err
	}

	var snapshot cloudcontrol.ProviderSnapshot
	if resource.DesiredState == domain.CloudResourceDesiredDeleted {
		snapshot, err = s.provider.Delete(ctx, *resource)
		if err == nil && snapshot.State != domain.CloudResourceDeleted {
			snapshot, err = s.provider.Observe(ctx, *resource)
		}
	} else {
		snapshot, err = s.provider.Apply(ctx, *resource)
	}
	if err != nil {
		_, recordErr := s.repo.RecordReconcileFailure(ctx, resource.ID, resource.Generation, boundedCloudError(err))
		if recordErr != nil {
			return fmt.Errorf("provider error: %v; record reconcile failure: %w", err, recordErr)
		}
		return err
	}

	converged := resource.DesiredState == domain.CloudResourceDesiredActive && snapshot.State == domain.CloudResourceActive
	converged = converged || (resource.DesiredState == domain.CloudResourceDesiredDeleted && snapshot.State == domain.CloudResourceDeleted)
	if !converged {
		_, err = s.repo.RecordReconcileProgress(ctx, resource.ID, resource.Generation, snapshot.State, snapshot.Spec, snapshot.ProviderResourceID)
		return err
	}
	_, err = s.repo.CompleteReconcile(ctx, resource.ID, resource.Generation, snapshot.State, snapshot.Spec, snapshot.ProviderResourceID, settledQuota)
	return err
}

func (s *CloudResourceService) ReconcilePending(ctx context.Context, limit int) (int, error) {
	if s.provider == nil {
		return 0, ErrCloudProviderDisabled
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	resources, err := s.repo.ListNeedsReconcile(ctx, time.Now().Add(-s.driftInterval), limit)
	if err != nil {
		return 0, err
	}
	reconciled := 0
	for _, resource := range resources {
		if err := s.ReconcileOne(ctx, resource.ID); err != nil {
			if errors.Is(err, ErrCloudReconcileBusy) {
				continue
			}
			log.Printf("cloud resource reconcile id=%d generation=%d: %v", resource.ID, resource.Generation, err)
			continue
		}
		reconciled++
	}
	return reconciled, nil
}

func (s *CloudResourceService) RunController(ctx context.Context, interval time.Duration, batch int) {
	if s.provider == nil {
		return
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	reconcile := func() {
		if _, err := s.ReconcilePending(ctx, batch); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("cloud resource reconcile loop: %v", err)
		}
	}
	reconcile()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

func (s *CloudResourceService) idempotentResult(ctx context.Context, key string, expectedResourceID uint64, expectedType domain.CloudOrderType, expectedTenant string) (*CloudMutationResult, error) {
	expectedTenant = strings.TrimSpace(expectedTenant)
	if expectedResourceID != 0 {
		resource, err := s.repo.GetResource(ctx, expectedResourceID)
		if err != nil {
			return nil, err
		}
		if expectedTenant != "" && expectedTenant != resource.TenantID {
			return nil, ErrIdempotencyConflict
		}
		expectedTenant = resource.TenantID
	}
	if expectedTenant == "" {
		return nil, ErrInvalidCloudResource
	}
	order, err := s.repo.FindOrderByTenantIdempotencyKey(ctx, expectedTenant, key)
	if err != nil {
		if errors.Is(err, repository.ErrCloudOrderNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if order.Type != expectedType || (expectedResourceID != 0 && order.ResourceID != expectedResourceID) {
		return nil, ErrIdempotencyConflict
	}
	resource, err := s.repo.GetResource(ctx, order.ResourceID)
	if err != nil {
		return nil, err
	}
	return &CloudMutationResult{Resource: resource, Order: order}, nil
}

func normalizeDeploymentSpec(raw json.RawMessage) (string, cloudcontrol.QuotaDemand, error) {
	if len(raw) == 0 {
		return "", cloudcontrol.QuotaDemand{}, ErrInvalidCloudResource
	}
	var compact json.RawMessage
	if err := json.Unmarshal(raw, &compact); err != nil {
		return "", cloudcontrol.QuotaDemand{}, ErrInvalidCloudResource
	}
	data, err := json.Marshal(compact)
	if err != nil {
		return "", cloudcontrol.QuotaDemand{}, ErrInvalidCloudResource
	}
	deploymentSpec, err := cloudcontrol.ParseKubernetesDeploymentSpec(string(data), "default")
	if err != nil {
		return "", cloudcontrol.QuotaDemand{}, fmt.Errorf("%w: %v", ErrInvalidCloudResource, err)
	}
	demand, err := cloudcontrol.DeploymentQuotaDemand(deploymentSpec)
	if err != nil {
		return "", cloudcontrol.QuotaDemand{}, fmt.Errorf("%w: %v", ErrInvalidCloudResource, err)
	}
	return string(data), demand, nil
}

func cloudTenantQuotaView(quota *domain.CloudTenantQuota) *CloudTenantQuotaView {
	if quota == nil {
		return nil
	}
	return &CloudTenantQuotaView{
		TenantID:           quota.TenantID,
		CPUMilliLimit:      quota.CPUMilliLimit,
		MemoryMiBLimit:     quota.MemoryMiBLimit,
		ReplicaLimit:       quota.ReplicaLimit,
		CPUMilliUsed:       quota.CPUMilliUsed,
		MemoryMiBUsed:      quota.MemoryMiBUsed,
		ReplicaUsed:        quota.ReplicaUsed,
		CPUMilliAvailable:  quota.CPUMilliLimit - quota.CPUMilliUsed,
		MemoryMiBAvailable: quota.MemoryMiBLimit - quota.MemoryMiBUsed,
		ReplicaAvailable:   quota.ReplicaLimit - quota.ReplicaUsed,
	}
}

func validKubernetesName(name string) bool {
	return len(name) > 0 && len(name) <= 63 && kubernetesNamePattern.MatchString(name)
}

func ensureIdempotencyKey(key string) string {
	key = strings.TrimSpace(key)
	if key != "" {
		if len(key) > 128 {
			return key[:128]
		}
		return key
	}
	var data [16]byte
	if _, err := rand.Read(data[:]); err == nil {
		return "auto-" + hex.EncodeToString(data[:])
	}
	return fmt.Sprintf("auto-%d", time.Now().UnixNano())
}

func boundedCloudError(err error) string {
	message := err.Error()
	if len(message) > 2048 {
		return message[:2048]
	}
	return message
}
