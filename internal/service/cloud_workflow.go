package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/repository"
)

var (
	ErrInvalidCloudWorkflow = errors.New("invalid cloud workflow")
	ErrCloudWorkflowBusy    = errors.New("cloud workflow is already being reconciled")
)

type CreateProvisionWorkflowInput struct {
	TenantID string                            `json:"tenant_id"`
	Name     string                            `json:"name"`
	Provider string                            `json:"provider"`
	Spec     cloudcontrol.KubernetesBundleSpec `json:"spec"`
}

type ResizeBundleWorkflowInput struct {
	Spec cloudcontrol.KubernetesBundleSpec `json:"spec"`
}

type persistedSagaStepResult struct {
	ExternalID string `json:"external_id,omitempty"`
	Output     string `json:"output,omitempty"`
}

type CloudWorkflowService struct {
	repo              *repository.CloudResourceRepository
	provider          cloudcontrol.SagaResourceProvider
	lease             time.Duration
	workerConcurrency int
	observeBackoff    time.Duration
	observeBackoffMax time.Duration
}

func NewCloudWorkflowService(repo *repository.CloudResourceRepository, provider cloudcontrol.SagaResourceProvider) *CloudWorkflowService {
	return &CloudWorkflowService{repo: repo, provider: provider, lease: 30 * time.Second, workerConcurrency: 1}
}

// ResourceTenant resolves ownership before mutation entrypoints. Authorization
// is enforced at the HTTP boundary; worker/controller reconciliation remains
// system-internal and does not inherit a caller's grant.
func (s *CloudWorkflowService) ResourceTenant(ctx context.Context, resourceID uint64) (string, error) {
	resource, err := s.repo.GetResource(ctx, resourceID)
	if err != nil {
		return "", err
	}
	return resource.TenantID, nil
}

func (s *CloudWorkflowService) SetLeaseDuration(lease time.Duration) {
	if lease > 0 {
		s.lease = lease
	}
}

func (s *CloudWorkflowService) SetWorkerConcurrency(workers int) {
	if workers > 0 {
		s.workerConcurrency = workers
	}
}

func (s *CloudWorkflowService) SetObserveBackoff(base, max time.Duration) {
	if base <= 0 {
		s.observeBackoff = 0
		s.observeBackoffMax = 0
		return
	}
	if max < base {
		max = base
	}
	s.observeBackoff = base
	s.observeBackoffMax = max
}

func (s *CloudWorkflowService) nextReconcileTime(state *repository.CloudWorkflowState, stepName string, compensation bool) (time.Time, bool) {
	if s.observeBackoff <= 0 || state == nil {
		return time.Time{}, false
	}
	attempt := 1
	for i := range state.Steps {
		if state.Steps[i].Name != stepName {
			continue
		}
		if compensation {
			attempt = state.Steps[i].CompensationAttemptCount + 1
		} else {
			attempt = state.Steps[i].AttemptCount + 1
		}
		break
	}
	delay := s.observeBackoff
	for i := 1; i < attempt && delay < s.observeBackoffMax; i++ {
		if delay > s.observeBackoffMax/2 {
			delay = s.observeBackoffMax
			break
		}
		delay *= 2
	}
	if delay > s.observeBackoffMax {
		delay = s.observeBackoffMax
	}
	return time.Now().Add(delay), true
}

func (s *CloudWorkflowService) CreateProvisionWorkflow(ctx context.Context, in CreateProvisionWorkflowInput, idempotencyKey string) (*repository.CloudWorkflowState, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentWorkflow(ctx, idempotencyKey, in); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	tenant := strings.TrimSpace(in.TenantID)
	name := strings.TrimSpace(in.Name)
	provider := strings.ToLower(strings.TrimSpace(in.Provider))
	if provider == "" {
		provider = "kubernetes"
	}
	if tenant == "" || !validKubernetesName(name) || provider != "kubernetes" {
		return nil, ErrInvalidCloudWorkflow
	}
	raw, err := json.Marshal(in.Spec)
	if err != nil {
		return nil, ErrInvalidCloudWorkflow
	}
	bundle, err := cloudcontrol.ParseKubernetesBundleSpec(string(raw), "default")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCloudWorkflow, err)
	}
	demand, err := cloudcontrol.BundleQuotaDemand(bundle)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCloudWorkflow, err)
	}
	computeSpec, err := cloudcontrol.KubernetesBundleComputeSpecJSON(bundle)
	if err != nil {
		return nil, err
	}
	workflowSpecBytes, err := json.Marshal(bundle)
	if err != nil {
		return nil, err
	}
	workflowSpec := string(workflowSpecBytes)

	resource := &domain.CloudResource{
		TenantID:          tenant,
		Name:              name,
		Kind:              "deployment",
		Provider:          provider,
		OrchestrationMode: domain.CloudOrchestrationSaga,
		DesiredSpec:       computeSpec,
		DesiredState:      domain.CloudResourceDesiredActive,
		ActualState:       domain.CloudResourcePending,
		Generation:        1,
	}
	order := &domain.CloudOrder{
		TenantID:       tenant,
		Type:           domain.CloudOrderCreate,
		Status:         domain.CloudOrderPending,
		IdempotencyKey: idempotencyKey,
		RequestedSpec:  workflowSpec,
	}
	workflow, _, err := s.repo.CreateProvisionWorkflow(ctx, resource, order, demand, workflowSpec)
	if err != nil {
		if existing, lookupErr := s.idempotentWorkflow(ctx, idempotencyKey, in); lookupErr == nil && existing != nil {
			return existing, nil
		}
		return nil, err
	}
	return s.repo.GetWorkflowState(ctx, workflow.ID)
}

func (s *CloudWorkflowService) CreateResizeWorkflow(ctx context.Context, resourceID uint64, in ResizeBundleWorkflowInput, idempotencyKey string) (*repository.CloudWorkflowState, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentMutationWorkflow(ctx, idempotencyKey, resourceID, domain.CloudOrderResize); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	resource, err := s.repo.GetResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	if resource.OrchestrationMode != domain.CloudOrchestrationSaga || resource.ActualState != domain.CloudResourceActive || resource.ActualBundleSpec == "" {
		return nil, repository.ErrCloudWorkflowRequiresActive
	}
	previous, err := cloudcontrol.ParseKubernetesBundleSpec(resource.ActualBundleSpec, "default")
	if err != nil {
		return nil, fmt.Errorf("decode current bundle: %w", err)
	}
	raw, err := json.Marshal(in.Spec)
	if err != nil {
		return nil, ErrInvalidCloudWorkflow
	}
	target, err := cloudcontrol.ParseKubernetesBundleSpec(string(raw), "default")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCloudWorkflow, err)
	}
	if target.Compute.Namespace != previous.Compute.Namespace {
		return nil, fmt.Errorf("%w: namespace is immutable during bundle resize", ErrInvalidCloudWorkflow)
	}
	if target.Storage != previous.Storage {
		return nil, fmt.Errorf("%w: storage configuration is immutable during bundle resize", ErrInvalidCloudWorkflow)
	}
	targetDemand, err := cloudcontrol.BundleQuotaDemand(target)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCloudWorkflow, err)
	}
	targetComputeSpec, err := cloudcontrol.KubernetesBundleComputeSpecJSON(target)
	if err != nil {
		return nil, err
	}
	targetBytes, err := json.Marshal(target)
	if err != nil {
		return nil, err
	}
	workflow, err := s.repo.CreateResizeWorkflow(
		ctx,
		resource.ID,
		&domain.CloudOrder{IdempotencyKey: idempotencyKey},
		resource.ActualBundleSpec,
		string(targetBytes),
		targetComputeSpec,
		targetDemand,
	)
	if err != nil {
		if existing, lookupErr := s.idempotentMutationWorkflow(ctx, idempotencyKey, resourceID, domain.CloudOrderResize); lookupErr == nil && existing != nil {
			return existing, nil
		}
		return nil, err
	}
	return s.repo.GetWorkflowState(ctx, workflow.ID)
}

func (s *CloudWorkflowService) CreateDeleteWorkflow(ctx context.Context, resourceID uint64, idempotencyKey string) (*repository.CloudWorkflowState, error) {
	if s.provider == nil {
		return nil, ErrCloudProviderDisabled
	}
	idempotencyKey = ensureIdempotencyKey(idempotencyKey)
	if existing, err := s.idempotentMutationWorkflow(ctx, idempotencyKey, resourceID, domain.CloudOrderDelete); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	resource, err := s.repo.GetResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	if resource.OrchestrationMode != domain.CloudOrchestrationSaga || resource.ActualState != domain.CloudResourceActive || resource.ActualBundleSpec == "" {
		return nil, repository.ErrCloudWorkflowRequiresActive
	}
	workflow, err := s.repo.CreateDeleteWorkflow(ctx, resource.ID, &domain.CloudOrder{IdempotencyKey: idempotencyKey}, resource.ActualBundleSpec)
	if err != nil {
		if existing, lookupErr := s.idempotentMutationWorkflow(ctx, idempotencyKey, resourceID, domain.CloudOrderDelete); lookupErr == nil && existing != nil {
			return existing, nil
		}
		return nil, err
	}
	return s.repo.GetWorkflowState(ctx, workflow.ID)
}

func (s *CloudWorkflowService) Get(ctx context.Context, id uint64) (*repository.CloudWorkflowState, error) {
	return s.repo.GetWorkflowState(ctx, id)
}

func (s *CloudWorkflowService) ListForResource(ctx context.Context, resourceID uint64, limit, offset int) ([]domain.CloudWorkflow, error) {
	if _, err := s.repo.GetResource(ctx, resourceID); err != nil {
		return nil, err
	}
	return s.repo.ListWorkflowsByResource(ctx, resourceID, limit, offset)
}

func (s *CloudWorkflowService) ReconcileOne(ctx context.Context, id uint64) error {
	if s.provider == nil {
		return ErrCloudProviderDisabled
	}
	state, err := s.repo.GetWorkflowState(ctx, id)
	if err != nil {
		return err
	}
	if workflowTerminal(state.Workflow.Status) {
		return nil
	}
	claimed, err := s.repo.ClaimWorkflow(ctx, id, time.Now().Add(s.lease))
	if err != nil {
		return err
	}
	if !claimed {
		metrics.CloudSagaClaims.WithLabelValues("busy").Inc()
		return ErrCloudWorkflowBusy
	}
	metrics.CloudSagaClaims.WithLabelValues("claimed").Inc()
	return s.reconcileClaimedOne(ctx, id)
}

func (s *CloudWorkflowService) reconcileClaimedOne(ctx context.Context, id uint64) error {
	state, err := s.repo.GetWorkflowState(ctx, id)
	if err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, id)
		return err
	}
	return s.reconcileClaimedState(ctx, state)
}

func (s *CloudWorkflowService) reconcileClaimedState(ctx context.Context, state *repository.CloudWorkflowState) error {
	id := state.Workflow.ID
	if workflowTerminal(state.Workflow.Status) {
		_ = s.repo.ReleaseWorkflowLease(ctx, id)
		return nil
	}
	bundle, err := cloudcontrol.ParseKubernetesBundleSpec(state.Workflow.Spec, "default")
	if err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, id)
		return err
	}
	if state.Workflow.Status == domain.CloudWorkflowCompensating {
		return s.compensateOne(ctx, state, bundle)
	}
	switch state.Workflow.Kind {
	case domain.CloudWorkflowKindProvision, domain.CloudWorkflowKindResize:
		return s.forwardOne(ctx, state, bundle)
	case domain.CloudWorkflowKindDelete:
		return s.forwardDeleteOne(ctx, state, bundle)
	default:
		_ = s.repo.ReleaseWorkflowLease(ctx, id)
		return fmt.Errorf("%w: unknown workflow kind %q", repository.ErrCloudWorkflowNotRunnable, state.Workflow.Kind)
	}
}

func (s *CloudWorkflowService) forwardOne(ctx context.Context, state *repository.CloudWorkflowState, bundle cloudcontrol.KubernetesBundleSpec) error {
	step := firstForwardStep(state.Steps)
	if step == nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return repository.ErrCloudWorkflowNotRunnable
	}

	switch step.Name {
	case domain.CloudWorkflowStepNetwork:
		if state.Workflow.Kind == domain.CloudWorkflowKindResize && !bundle.Network.Enabled {
			return s.runProviderForwardStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
				return s.provider.DeleteNetwork(ctx, *state.Resource, bundle)
			})
		}
		if !bundle.Network.Enabled {
			return s.repo.CompleteForwardStep(ctx, state.Workflow.ID, step.Name, `{"enabled":false}`, true)
		}
		return s.runProviderForwardStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.EnsureNetwork(ctx, *state.Resource, bundle)
		})

	case domain.CloudWorkflowStepStorage:
		if !bundle.Storage.Enabled {
			return s.repo.CompleteForwardStep(ctx, state.Workflow.ID, step.Name, `{"enabled":false}`, true)
		}
		return s.runProviderForwardStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.EnsureStorage(ctx, *state.Resource, bundle)
		})

	case domain.CloudWorkflowStepCompute:
		return s.runProviderForwardStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.EnsureCompute(ctx, *state.Resource, bundle)
		})

	case domain.CloudWorkflowStepCommit:
		compute := findWorkflowStep(state.Steps, domain.CloudWorkflowStepCompute)
		if compute == nil || compute.Status != domain.CloudWorkflowStepSucceeded {
			_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
			return fmt.Errorf("%w: compute step has not succeeded", repository.ErrCloudWorkflowNotRunnable)
		}
		result, err := decodePersistedSagaStepResult(compute.Output)
		if err != nil {
			_ = s.repo.FailForwardStep(ctx, state.Workflow.ID, step.Name, boundedCloudError(err))
			return err
		}
		if state.Workflow.Kind == domain.CloudWorkflowKindResize {
			demand, demandErr := cloudcontrol.BundleQuotaDemand(bundle)
			if demandErr != nil {
				return demandErr
			}
			return s.repo.CommitResizeWorkflow(ctx, state.Workflow.ID, result.ExternalID, result.Output, demand)
		}
		return s.repo.CommitProvisionWorkflow(ctx, state.Workflow.ID, result.ExternalID, result.Output)

	case domain.CloudWorkflowStepQuota:
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return fmt.Errorf("%w: quota step unexpectedly runnable", repository.ErrCloudWorkflowNotRunnable)
	default:
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return fmt.Errorf("%w: unknown step %q", repository.ErrCloudWorkflowNotRunnable, step.Name)
	}
}

func (s *CloudWorkflowService) forwardDeleteOne(ctx context.Context, state *repository.CloudWorkflowState, bundle cloudcontrol.KubernetesBundleSpec) error {
	step := firstForwardStep(state.Steps)
	if step == nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return repository.ErrCloudWorkflowNotRunnable
	}

	switch step.Name {
	case domain.CloudWorkflowStepCompute:
		return s.runDeleteStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.DeleteCompute(ctx, *state.Resource, bundle)
		})
	case domain.CloudWorkflowStepStorage:
		return s.runDeleteStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.DeleteStorage(ctx, *state.Resource, bundle)
		})
	case domain.CloudWorkflowStepNetwork:
		return s.runDeleteStep(ctx, state, step.Name, func() (cloudcontrol.SagaStepResult, error) {
			return s.provider.DeleteNetwork(ctx, *state.Resource, bundle)
		})
	case domain.CloudWorkflowStepQuota:
		return s.repo.CompleteDeleteQuotaStep(ctx, state.Workflow.ID)
	case domain.CloudWorkflowStepCommit:
		return s.repo.CommitDeleteWorkflow(ctx, state.Workflow.ID)
	default:
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return fmt.Errorf("%w: unknown delete step %q", repository.ErrCloudWorkflowNotRunnable, step.Name)
	}
}

func (s *CloudWorkflowService) runDeleteStep(
	ctx context.Context,
	state *repository.CloudWorkflowState,
	stepName string,
	call func() (cloudcontrol.SagaStepResult, error),
) error {
	started := time.Now()
	defer func() {
		metrics.CloudSagaStepDuration.WithLabelValues(state.Workflow.Kind, stepName, "delete").Observe(time.Since(started).Seconds())
	}()
	if err := s.repo.MarkForwardStepRunning(ctx, state.Workflow.ID, stepName); err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return err
	}
	result, err := call()
	if err != nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "delete", "error").Inc()
		message := boundedCloudError(err)
		nextAt, scheduled := s.nextReconcileTime(state, stepName, false)
		var recordErr error
		if scheduled {
			recordErr = s.repo.RecordForwardRetryableFailure(ctx, state.Workflow.ID, stepName, message, nextAt)
		} else {
			recordErr = s.repo.RecordForwardRetryableFailure(ctx, state.Workflow.ID, stepName, message)
		}
		if recordErr != nil {
			return fmt.Errorf("delete provider error: %v; persist retryable failure: %w", err, recordErr)
		}
		return err
	}
	if !result.Ready {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "delete", "pending").Inc()
		if nextAt, scheduled := s.nextReconcileTime(state, stepName, false); scheduled {
			return s.repo.RecordForwardStepProgress(ctx, state.Workflow.ID, stepName, result.Output, nextAt)
		}
		return s.repo.RecordForwardStepProgress(ctx, state.Workflow.ID, stepName, result.Output)
	}
	err = s.repo.CompleteForwardStep(ctx, state.Workflow.ID, stepName, result.Output, false)
	if err == nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "delete", "succeeded").Inc()
	}
	return err
}

func (s *CloudWorkflowService) runProviderForwardStep(
	ctx context.Context,
	state *repository.CloudWorkflowState,
	stepName string,
	call func() (cloudcontrol.SagaStepResult, error),
) error {
	started := time.Now()
	defer func() {
		metrics.CloudSagaStepDuration.WithLabelValues(state.Workflow.Kind, stepName, "forward").Observe(time.Since(started).Seconds())
	}()
	if err := s.repo.MarkForwardStepRunning(ctx, state.Workflow.ID, stepName); err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return err
	}
	result, err := call()
	if err != nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "forward", "error").Inc()
		message := boundedCloudError(err)
		if recordErr := s.repo.FailForwardStep(ctx, state.Workflow.ID, stepName, message); recordErr != nil {
			return fmt.Errorf("provider error: %v; enter saga compensation: %w", err, recordErr)
		}
		return err
	}
	persisted, err := encodePersistedSagaStepResult(result)
	if err != nil {
		_ = s.repo.FailForwardStep(ctx, state.Workflow.ID, stepName, boundedCloudError(err))
		return err
	}
	if !result.Ready {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "forward", "pending").Inc()
		if nextAt, scheduled := s.nextReconcileTime(state, stepName, false); scheduled {
			return s.repo.RecordForwardStepProgress(ctx, state.Workflow.ID, stepName, persisted, nextAt)
		}
		return s.repo.RecordForwardStepProgress(ctx, state.Workflow.ID, stepName, persisted)
	}
	err = s.repo.CompleteForwardStep(ctx, state.Workflow.ID, stepName, persisted, false)
	if err == nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, stepName, "forward", "succeeded").Inc()
	}
	return err
}

func (s *CloudWorkflowService) compensateOne(ctx context.Context, state *repository.CloudWorkflowState, bundle cloudcontrol.KubernetesBundleSpec) error {
	if state.Workflow.Kind == domain.CloudWorkflowKindResize {
		return s.compensateResizeOne(ctx, state, bundle)
	}
	step := firstCompensationStep(state.Steps)
	if step == nil {
		return s.repo.FinalizeProvisionCompensation(ctx, state.Workflow.ID)
	}
	if step.Name == domain.CloudWorkflowStepQuota {
		return s.repo.FinalizeProvisionCompensation(ctx, state.Workflow.ID)
	}
	if err := s.repo.MarkCompensationRunning(ctx, state.Workflow.ID, step.Name); err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return err
	}
	started := time.Now()
	defer func() {
		metrics.CloudSagaStepDuration.WithLabelValues(state.Workflow.Kind, step.Name, "compensate").Observe(time.Since(started).Seconds())
	}()

	var result cloudcontrol.SagaStepResult
	var err error
	switch step.Name {
	case domain.CloudWorkflowStepCompute:
		result, err = s.provider.DeleteCompute(ctx, *state.Resource, bundle)
	case domain.CloudWorkflowStepStorage:
		result, err = s.provider.DeleteStorage(ctx, *state.Resource, bundle)
	case domain.CloudWorkflowStepNetwork:
		result, err = s.provider.DeleteNetwork(ctx, *state.Resource, bundle)
	default:
		err = fmt.Errorf("unknown compensating step %q", step.Name)
	}
	if err != nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "error").Inc()
		message := boundedCloudError(err)
		nextAt, scheduled := s.nextReconcileTime(state, step.Name, true)
		var recordErr error
		if scheduled {
			recordErr = s.repo.RecordCompensationFailure(ctx, state.Workflow.ID, step.Name, message, nextAt)
		} else {
			recordErr = s.repo.RecordCompensationFailure(ctx, state.Workflow.ID, step.Name, message)
		}
		if recordErr != nil {
			return fmt.Errorf("compensation error: %v; persist compensation failure: %w", err, recordErr)
		}
		return err
	}
	if !result.Ready {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "pending").Inc()
		if nextAt, scheduled := s.nextReconcileTime(state, step.Name, true); scheduled {
			return s.repo.RecordCompensationProgress(ctx, state.Workflow.ID, step.Name, nextAt)
		}
		return s.repo.RecordCompensationProgress(ctx, state.Workflow.ID, step.Name)
	}
	err = s.repo.CompleteCompensationStep(ctx, state.Workflow.ID, step.Name)
	if err == nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "succeeded").Inc()
	}
	return err
}

func (s *CloudWorkflowService) compensateResizeOne(ctx context.Context, state *repository.CloudWorkflowState, target cloudcontrol.KubernetesBundleSpec) error {
	previous, err := cloudcontrol.ParseKubernetesBundleSpec(state.Workflow.PreviousSpec, "default")
	if err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return err
	}
	step := firstCompensationStep(state.Steps)
	if step == nil || step.Name == domain.CloudWorkflowStepQuota {
		previousDemand, demandErr := cloudcontrol.BundleQuotaDemand(previous)
		if demandErr != nil {
			return demandErr
		}
		previousComputeSpec, specErr := cloudcontrol.KubernetesBundleComputeSpecJSON(previous)
		if specErr != nil {
			return specErr
		}
		return s.repo.FinalizeResizeCompensation(ctx, state.Workflow.ID, previousDemand, previousComputeSpec)
	}
	if err := s.repo.MarkCompensationRunning(ctx, state.Workflow.ID, step.Name); err != nil {
		_ = s.repo.ReleaseWorkflowLease(ctx, state.Workflow.ID)
		return err
	}
	started := time.Now()
	defer func() {
		metrics.CloudSagaStepDuration.WithLabelValues(state.Workflow.Kind, step.Name, "compensate").Observe(time.Since(started).Seconds())
	}()

	var result cloudcontrol.SagaStepResult
	switch step.Name {
	case domain.CloudWorkflowStepCompute:
		previousResource := *state.Resource
		previousResource.DesiredState = domain.CloudResourceDesiredActive
		previousResource.DesiredSpec, err = cloudcontrol.KubernetesBundleComputeSpecJSON(previous)
		if err == nil {
			result, err = s.provider.EnsureCompute(ctx, previousResource, previous)
		}
	case domain.CloudWorkflowStepNetwork:
		if previous.Network.Enabled {
			result, err = s.provider.EnsureNetwork(ctx, *state.Resource, previous)
		} else {
			result, err = s.provider.DeleteNetwork(ctx, *state.Resource, target)
		}
	default:
		err = fmt.Errorf("unknown resize compensating step %q", step.Name)
	}
	if err != nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "error").Inc()
		message := boundedCloudError(err)
		nextAt, scheduled := s.nextReconcileTime(state, step.Name, true)
		var recordErr error
		if scheduled {
			recordErr = s.repo.RecordCompensationFailure(ctx, state.Workflow.ID, step.Name, message, nextAt)
		} else {
			recordErr = s.repo.RecordCompensationFailure(ctx, state.Workflow.ID, step.Name, message)
		}
		if recordErr != nil {
			return fmt.Errorf("resize compensation error: %v; persist failure: %w", err, recordErr)
		}
		return err
	}
	if !result.Ready {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "pending").Inc()
		if nextAt, scheduled := s.nextReconcileTime(state, step.Name, true); scheduled {
			return s.repo.RecordCompensationProgress(ctx, state.Workflow.ID, step.Name, nextAt)
		}
		return s.repo.RecordCompensationProgress(ctx, state.Workflow.ID, step.Name)
	}
	err = s.repo.CompleteCompensationStep(ctx, state.Workflow.ID, step.Name)
	if err == nil {
		metrics.CloudSagaStepOperations.WithLabelValues(state.Workflow.Kind, step.Name, "compensate", "succeeded").Inc()
	}
	return err
}

func (s *CloudWorkflowService) ReconcilePending(ctx context.Context, limit int) (int, error) {
	if s.repo.WorkflowDBLegacy() {
		workflows, err := s.repo.ListRunnableWorkflows(ctx, limit)
		if err != nil {
			return 0, err
		}
		metrics.CloudSagaRunnable.Set(float64(len(workflows)))
		processed := 0
		for _, workflow := range workflows {
			if err := s.ReconcileOne(ctx, workflow.ID); err != nil {
				if !errors.Is(err, ErrCloudWorkflowBusy) {
					log.Printf("cloud saga legacy reconcile workflow=%d status=%s: %v", workflow.ID, workflow.Status, err)
				}
				continue
			}
			processed++
		}
		return processed, nil
	}
	workflows, err := s.repo.ClaimRunnableWorkflows(ctx, limit, time.Now().Add(s.lease))
	if err != nil {
		return 0, err
	}
	metrics.CloudSagaRunnable.Set(float64(len(workflows)))
	if len(workflows) > 0 {
		metrics.CloudSagaClaims.WithLabelValues("claimed").Add(float64(len(workflows)))
	}
	processed := 0
	if s.repo.WorkflowDBOptimized() && len(workflows) > 0 {
		states, loadErr := s.repo.GetClaimedWorkflowStates(ctx, workflows)
		if loadErr != nil {
			ids := make([]uint64, 0, len(workflows))
			for i := range workflows {
				ids = append(ids, workflows[i].ID)
			}
			_ = s.repo.ReleaseWorkflowLeases(ctx, ids)
			return 0, loadErr
		}
		workers := s.workerConcurrency
		if workers <= 1 || len(states) <= 1 {
			for _, state := range states {
				if err := s.reconcileClaimedState(ctx, state); err != nil {
					log.Printf("cloud saga reconcile workflow=%d status=%s: %v", state.Workflow.ID, state.Workflow.Status, err)
					continue
				}
				processed++
			}
			return processed, nil
		}
		if workers > len(states) {
			workers = len(states)
		}
		jobs := make(chan *repository.CloudWorkflowState)
		var wg sync.WaitGroup
		var mu sync.Mutex
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for state := range jobs {
					if err := s.reconcileClaimedState(ctx, state); err != nil {
						log.Printf("cloud saga reconcile workflow=%d status=%s: %v", state.Workflow.ID, state.Workflow.Status, err)
						continue
					}
					mu.Lock()
					processed++
					mu.Unlock()
				}
			}()
		}
		for _, state := range states {
			jobs <- state
		}
		close(jobs)
		wg.Wait()
		return processed, nil
	}
	for _, workflow := range workflows {
		if err := s.reconcileClaimedOne(ctx, workflow.ID); err != nil {
			log.Printf("cloud saga reconcile workflow=%d status=%s: %v", workflow.ID, workflow.Status, err)
			continue
		}
		processed++
	}
	return processed, nil
}

func (s *CloudWorkflowService) RunController(ctx context.Context, interval time.Duration, batch int) {
	if s.provider == nil {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	run := func() {
		if _, err := s.ReconcilePending(ctx, batch); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("cloud saga reconcile loop: %v", err)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (s *CloudWorkflowService) idempotentWorkflow(ctx context.Context, key string, in CreateProvisionWorkflowInput) (*repository.CloudWorkflowState, error) {
	tenant := strings.TrimSpace(in.TenantID)
	if tenant == "" {
		return nil, ErrInvalidCloudWorkflow
	}
	order, err := s.repo.FindOrderByTenantIdempotencyKey(ctx, tenant, key)
	if err != nil {
		if errors.Is(err, repository.ErrCloudOrderNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if order.Type != domain.CloudOrderCreate {
		return nil, ErrIdempotencyConflict
	}
	workflow, err := s.repo.FindWorkflowByOrderID(ctx, order.ID)
	if err != nil {
		if errors.Is(err, repository.ErrCloudWorkflowNotFound) {
			return nil, ErrIdempotencyConflict
		}
		return nil, err
	}
	state, err := s.repo.GetWorkflowState(ctx, workflow.ID)
	if err != nil {
		return nil, err
	}
	if state.Resource.TenantID != strings.TrimSpace(in.TenantID) || state.Resource.Name != strings.TrimSpace(in.Name) {
		return nil, ErrIdempotencyConflict
	}
	return state, nil
}

func (s *CloudWorkflowService) idempotentMutationWorkflow(ctx context.Context, key string, resourceID uint64, orderType domain.CloudOrderType) (*repository.CloudWorkflowState, error) {
	resource, err := s.repo.GetResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	order, err := s.repo.FindOrderByTenantIdempotencyKey(ctx, resource.TenantID, key)
	if err != nil {
		if errors.Is(err, repository.ErrCloudOrderNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if order.Type != orderType || order.ResourceID != resourceID {
		return nil, ErrIdempotencyConflict
	}
	workflow, err := s.repo.FindWorkflowByOrderID(ctx, order.ID)
	if err != nil {
		return nil, err
	}
	return s.repo.GetWorkflowState(ctx, workflow.ID)
}

func workflowTerminal(status domain.CloudWorkflowStatus) bool {
	return status == domain.CloudWorkflowSucceeded || status == domain.CloudWorkflowCompensated
}

func firstForwardStep(steps []domain.CloudWorkflowStep) *domain.CloudWorkflowStep {
	for i := range steps {
		if steps[i].Status == domain.CloudWorkflowStepPending || steps[i].Status == domain.CloudWorkflowStepRunning {
			return &steps[i]
		}
	}
	return nil
}

func firstCompensationStep(steps []domain.CloudWorkflowStep) *domain.CloudWorkflowStep {
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].Name != domain.CloudWorkflowStepCommit && (steps[i].Status == domain.CloudWorkflowStepSucceeded || steps[i].Status == domain.CloudWorkflowStepFailed || steps[i].Status == domain.CloudWorkflowStepCompensating) {
			return &steps[i]
		}
	}
	return nil
}

func findWorkflowStep(steps []domain.CloudWorkflowStep, name string) *domain.CloudWorkflowStep {
	for i := range steps {
		if steps[i].Name == name {
			return &steps[i]
		}
	}
	return nil
}

func encodePersistedSagaStepResult(result cloudcontrol.SagaStepResult) (string, error) {
	data, err := json.Marshal(persistedSagaStepResult{ExternalID: result.ExternalID, Output: result.Output})
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodePersistedSagaStepResult(raw string) (persistedSagaStepResult, error) {
	var result persistedSagaStepResult
	if strings.TrimSpace(raw) == "" {
		return result, errors.New("missing compute step result")
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return result, fmt.Errorf("decode compute step result: %w", err)
	}
	return result, nil
}
