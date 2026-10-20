package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/testsupport"
)

type sagaFakeProvider struct {
	mu                    sync.Mutex
	failForwardStep       string
	failComputeReplicas   int32
	failDeleteNetworkOnce bool
	failDeleteStorageOnce bool
	deleteNetworkAttempts int
	deleteStorageAttempts int
	ensureNetworkCalls    int
	ensureComputeCalls    int
}

func (p *sagaFakeProvider) EnsureNetwork(context.Context, domain.CloudResource, cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	p.mu.Lock()
	p.ensureNetworkCalls++
	p.mu.Unlock()
	if p.failForwardStep == domain.CloudWorkflowStepNetwork {
		return cloudcontrol.SagaStepResult{}, errors.New("injected network failure")
	}
	return cloudcontrol.SagaStepResult{Ready: true, ExternalID: "net-1", Output: `{"service":"ready"}`}, nil
}

func (p *sagaFakeProvider) DeleteNetwork(context.Context, domain.CloudResource, cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleteNetworkAttempts++
	if p.failDeleteNetworkOnce && p.deleteNetworkAttempts == 1 {
		return cloudcontrol.SagaStepResult{}, errors.New("injected network compensation failure")
	}
	return cloudcontrol.SagaStepResult{Ready: true}, nil
}

func (p *sagaFakeProvider) EnsureStorage(context.Context, domain.CloudResource, cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	if p.failForwardStep == domain.CloudWorkflowStepStorage {
		return cloudcontrol.SagaStepResult{}, errors.New("injected storage failure")
	}
	return cloudcontrol.SagaStepResult{Ready: true, ExternalID: "pvc-1", Output: `{"pvc":"ready"}`}, nil
}

func (p *sagaFakeProvider) DeleteStorage(context.Context, domain.CloudResource, cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleteStorageAttempts++
	if p.failDeleteStorageOnce && p.deleteStorageAttempts == 1 {
		return cloudcontrol.SagaStepResult{}, errors.New("injected storage delete failure")
	}
	return cloudcontrol.SagaStepResult{Ready: true}, nil
}

func (p *sagaFakeProvider) EnsureCompute(_ context.Context, _ domain.CloudResource, bundle cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	p.mu.Lock()
	p.ensureComputeCalls++
	p.mu.Unlock()
	if p.failForwardStep == domain.CloudWorkflowStepCompute {
		return cloudcontrol.SagaStepResult{}, errors.New("injected compute failure")
	}
	if p.failComputeReplicas > 0 && bundle.Compute.Replicas == p.failComputeReplicas {
		return cloudcontrol.SagaStepResult{}, errors.New("injected resize compute failure")
	}
	return cloudcontrol.SagaStepResult{
		Ready:      true,
		ExternalID: "deploy-1",
		Output:     fmt.Sprintf(`{"available_replicas":%d}`, bundle.Compute.Replicas),
	}, nil
}

func (p *sagaFakeProvider) DeleteCompute(context.Context, domain.CloudResource, cloudcontrol.KubernetesBundleSpec) (cloudcontrol.SagaStepResult, error) {
	return cloudcontrol.SagaStepResult{Ready: true}, nil
}

func TestCloudWorkflowSagaIntegration(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("TASKFLOW_MYSQL_INTEGRATION_DSN is not set")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("unsafe integration DSN: database must be an explicit *_ci or *_test schema")
	}

	db, err := repository.NewMySQL(dsn, 20, 10, true)
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(
		&domain.CloudResource{},
		&domain.CloudOrder{},
		&domain.CloudTenantQuota{},
		&domain.CloudTenantQuotaBucket{},
		&domain.CloudWorkflow{},
		&domain.CloudWorkflowStep{},
	); err != nil {
		t.Fatalf("migrate saga models: %v", err)
	}
	if err := repository.NormalizeCloudWorkflowIndexes(db); err != nil {
		t.Fatalf("normalize saga indexes: %v", err)
	}

	repo := repository.NewCloudResourceRepository(db)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	t.Run("late quota reservation rolls back the whole provision transaction", func(t *testing.T) {
		tenant := "saga-quota-rollback-" + suffix
		key := "saga-quota-rollback-key-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 999, MemoryMiB: 511, Replicas: 1}); err != nil {
			t.Fatal(err)
		}
		coordinator := NewCloudWorkflowService(repo, &sagaFakeProvider{})
		if _, err := coordinator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "quota-rollback-"+suffix), key); !errors.Is(err, repository.ErrCloudQuotaExceeded) {
			t.Fatalf("create error=%v want quota exceeded", err)
		}
		quota := mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 0 || quota.MemoryMiBUsed != 0 || quota.ReplicaUsed != 0 {
			t.Fatalf("quota changed after rolled-back admission: %+v", quota)
		}
		if _, err := repo.FindOrderByIdempotencyKey(ctx, key); !errors.Is(err, repository.ErrCloudOrderNotFound) {
			t.Fatalf("rolled-back order lookup err=%v want not found", err)
		}
		var resources, workflows, steps int64
		if err := db.Model(&domain.CloudResource{}).Where("tenant_id = ?", tenant).Count(&resources).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&domain.CloudWorkflow{}).Where("tenant_id = ?", tenant).Count(&workflows).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table("cloud_workflow_steps AS s").
			Joins("JOIN cloud_workflows AS w ON w.id = s.workflow_id").
			Where("w.tenant_id = ?", tenant).
			Count(&steps).Error; err != nil {
			t.Fatal(err)
		}
		if resources != 0 || workflows != 0 || steps != 0 {
			t.Fatalf("late quota failure leaked rows resources=%d workflows=%d steps=%d", resources, workflows, steps)
		}
	})

	t.Run("concurrent controllers claim disjoint runnable batches", func(t *testing.T) {
		if err := db.Exec("SET FOREIGN_KEY_CHECKS=0").Error; err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"cloud_workflow_steps", "cloud_workflows", "cloud_orders", "cloud_resources", "cloud_tenant_quotas"} {
			if err := db.Exec("DELETE FROM " + table).Error; err != nil {
				t.Fatalf("clear %s: %v", table, err)
			}
		}
		if err := db.Exec("SET FOREIGN_KEY_CHECKS=1").Error; err != nil {
			t.Fatal(err)
		}
		tenant := "saga-batch-claim-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 16000, MemoryMiB: 8192, Replicas: 32}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		creator := NewCloudWorkflowService(repo, provider)
		workflowIDs := make([]uint64, 0, 8)
		for i := 0; i < 8; i++ {
			state, err := creator.CreateProvisionWorkflow(
				ctx,
				sagaWorkflowInput(tenant, fmt.Sprintf("batch-claim-%d-%s", i, suffix)),
				fmt.Sprintf("batch-claim-key-%d-%s", i, suffix),
			)
			if err != nil {
				t.Fatalf("create workflow %d: %v", i, err)
			}
			workflowIDs = append(workflowIDs, state.Workflow.ID)
		}

		controllers := []*CloudWorkflowService{
			NewCloudWorkflowService(repo, provider),
			NewCloudWorkflowService(repo, provider),
		}
		start := make(chan struct{})
		type result struct {
			processed int
			err       error
		}
		results := make(chan result, len(controllers))
		for _, controller := range controllers {
			controller := controller
			go func() {
				<-start
				processed, err := controller.ReconcilePending(ctx, 4)
				results <- result{processed: processed, err: err}
			}()
		}
		close(start)
		total := 0
		for range controllers {
			result := <-results
			if result.err != nil {
				t.Fatalf("batch reconcile: %v", result.err)
			}
			total += result.processed
		}
		if total != len(workflowIDs) {
			t.Fatalf("processed=%d want=%d", total, len(workflowIDs))
		}
		for _, workflowID := range workflowIDs {
			state, err := repo.GetWorkflowState(ctx, workflowID)
			if err != nil {
				t.Fatal(err)
			}
			step := findWorkflowStep(state.Steps, domain.CloudWorkflowStepNetwork)
			if step == nil || step.Status != domain.CloudWorkflowStepSucceeded || step.AttemptCount != 1 {
				t.Fatalf("workflow=%d network step=%+v", workflowID, step)
			}
		}
		provider.mu.Lock()
		networkCalls := provider.ensureNetworkCalls
		provider.mu.Unlock()
		if networkCalls != len(workflowIDs) {
			t.Fatalf("network calls=%d want=%d", networkCalls, len(workflowIDs))
		}
	})

	t.Run("persisted next reconcile gate survives controller handoff", func(t *testing.T) {
		if err := db.Exec("SET FOREIGN_KEY_CHECKS=0").Error; err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"cloud_workflow_steps", "cloud_workflows", "cloud_orders", "cloud_resources", "cloud_tenant_quotas"} {
			if err := db.Exec("DELETE FROM " + table).Error; err != nil {
				t.Fatalf("clear %s: %v", table, err)
			}
		}
		if err := db.Exec("SET FOREIGN_KEY_CHECKS=1").Error; err != nil {
			t.Fatal(err)
		}

		tenant := "saga-backoff-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 4000, MemoryMiB: 4096, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		creator := NewCloudWorkflowService(repo, provider)
		state, err := creator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "backoff-"+suffix), "saga-backoff-key-"+suffix)
		if err != nil {
			t.Fatal(err)
		}
		future := time.Now().Add(time.Minute)
		if err := db.Model(&domain.CloudWorkflow{}).
			Where("id = ?", state.Workflow.ID).
			Updates(map[string]any{"next_reconcile_at": future, "lease_until": nil}).Error; err != nil {
			t.Fatal(err)
		}

		controllerA := NewCloudWorkflowService(repo, provider)
		processed, err := controllerA.ReconcilePending(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if processed != 0 {
			t.Fatalf("processed=%d want=0 while next_reconcile_at is in the future", processed)
		}
		controllerB := NewCloudWorkflowService(repo, provider)
		if err := controllerB.ReconcileOne(ctx, state.Workflow.ID); !errors.Is(err, ErrCloudWorkflowBusy) {
			t.Fatalf("direct reconcile before due err=%v want busy", err)
		}
		provider.mu.Lock()
		callsBeforeDue := provider.ensureNetworkCalls
		provider.mu.Unlock()
		if callsBeforeDue != 0 {
			t.Fatalf("provider calls before due=%d want=0", callsBeforeDue)
		}

		past := time.Now().Add(-time.Second)
		if err := db.Model(&domain.CloudWorkflow{}).
			Where("id = ?", state.Workflow.ID).
			Update("next_reconcile_at", past).Error; err != nil {
			t.Fatal(err)
		}
		processed, err = controllerB.ReconcilePending(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if processed != 1 {
			t.Fatalf("processed=%d want=1 after backoff due", processed)
		}
		state, err = repo.GetWorkflowState(ctx, state.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Workflow.NextReconcileAt != nil {
			t.Fatalf("next_reconcile_at=%v want nil after successful step", state.Workflow.NextReconcileAt)
		}
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepSucceeded)
	})

	t.Run("resume after restart and commit", func(t *testing.T) {
		tenant := "saga-ok-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 4000, MemoryMiB: 4096, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		serviceBeforeRestart := NewCloudWorkflowService(repo, provider)
		state, err := serviceBeforeRestart.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "bundle-ok-"+suffix), "saga-ok-key-"+suffix)
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		workflowID := state.Workflow.ID
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepQuota, domain.CloudWorkflowStepSucceeded)
		quota := mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 1000 || quota.MemoryMiBUsed != 512 || quota.ReplicaUsed != 2 {
			t.Fatalf("quota not reserved at saga admission: %+v", quota)
		}

		// First controller instance completes exactly one durable transition.
		if err := serviceBeforeRestart.ReconcileOne(ctx, workflowID); err != nil {
			t.Fatalf("network step: %v", err)
		}
		state, _ = repo.GetWorkflowState(ctx, workflowID)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepSucceeded)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepPending)

		// Simulate process restart: a fresh coordinator resumes from MySQL and
		// must not execute the already-succeeded network step again.
		serviceAfterRestart := NewCloudWorkflowService(repo, provider)
		for i := 0; i < 3; i++ {
			if err := serviceAfterRestart.ReconcileOne(ctx, workflowID); err != nil {
				t.Fatalf("resume tick %d: %v", i, err)
			}
		}
		state, err = repo.GetWorkflowState(ctx, workflowID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Workflow.Status != domain.CloudWorkflowSucceeded || state.Order.Status != domain.CloudOrderSucceeded || state.Resource.ActualState != domain.CloudResourceActive {
			t.Fatalf("workflow did not commit: workflow=%s order=%s resource=%s", state.Workflow.Status, state.Order.Status, state.Resource.ActualState)
		}
		for _, name := range []string{domain.CloudWorkflowStepQuota, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepCompute, domain.CloudWorkflowStepCommit} {
			assertSagaStepStatus(t, state, name, domain.CloudWorkflowStepSucceeded)
		}

		candidates, err := repo.ListNeedsReconcile(ctx, time.Now().Add(time.Hour), 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range candidates {
			if candidate.ID == state.Resource.ID {
				t.Fatal("saga-managed resource leaked into the direct reconciler")
			}
		}
	})

	t.Run("provision resize rollback and fail-forward delete lifecycle", func(t *testing.T) {
		tenant := "saga-life-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 6000, MemoryMiB: 8192, Replicas: 10}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		coordinator := NewCloudWorkflowService(repo, provider)

		// 1) Provision the initial bundle.
		state, err := coordinator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "bundle-life-"+suffix), "saga-life-create-"+suffix)
		if err != nil {
			t.Fatalf("create provision workflow: %v", err)
		}
		for i := 0; i < 4; i++ {
			if err := coordinator.ReconcileOne(ctx, state.Workflow.ID); err != nil {
				t.Fatalf("provision tick %d: %v", i, err)
			}
		}
		state, err = repo.GetWorkflowState(ctx, state.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		resourceID := state.Resource.ID
		if state.Resource.ActualBundleSpec == "" || state.Resource.DesiredBundleSpec == "" {
			t.Fatal("provision commit did not persist desired/actual bundle specs")
		}

		// 2) Upsize. Extra quota must be reserved atomically before the external
		// Service/Deployment mutations start.
		resizeUp := resizeSagaWorkflowInput(3, "600m", "341Mi", 81)
		resizeState, err := coordinator.CreateResizeWorkflow(ctx, resourceID, resizeUp, "saga-life-resize-up-"+suffix)
		if err != nil {
			t.Fatalf("create resize-up workflow: %v", err)
		}
		quota := mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 1800 || quota.MemoryMiBUsed != 1023 || quota.ReplicaUsed != 3 {
			t.Fatalf("resize-up quota was not precharged: %+v", quota)
		}
		if _, err := coordinator.CreateResizeWorkflow(ctx, resourceID, resizeUp, "saga-life-overlap-"+suffix); !errors.Is(err, repository.ErrCloudWorkflowRequiresActive) && !errors.Is(err, repository.ErrCloudWorkflowActive) {
			t.Fatalf("expected overlapping mutation rejection, got %v", err)
		}
		for i := 0; i < 3; i++ {
			if err := coordinator.ReconcileOne(ctx, resizeState.Workflow.ID); err != nil {
				t.Fatalf("resize-up tick %d: %v", i, err)
			}
		}
		resizeState, err = repo.GetWorkflowState(ctx, resizeState.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if resizeState.Workflow.Status != domain.CloudWorkflowSucceeded || resizeState.Resource.Generation != 2 || resizeState.Resource.ObservedGeneration != 2 {
			t.Fatalf("resize-up did not converge: workflow=%s generation=%d observed=%d", resizeState.Workflow.Status, resizeState.Resource.Generation, resizeState.Resource.ObservedGeneration)
		}

		// 3) A larger resize fails during compute. The target quota is held while
		// compensation restores the old Deployment and Service, then additional
		// quota is released back to the last successful bundle.
		provider.failComputeReplicas = 4
		resizeFail := resizeSagaWorkflowInput(4, "600m", "341Mi", 82)
		failedState, err := coordinator.CreateResizeWorkflow(ctx, resourceID, resizeFail, "saga-life-resize-fail-"+suffix)
		if err != nil {
			t.Fatalf("create failing resize: %v", err)
		}
		quota = mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 2400 || quota.MemoryMiBUsed != 1364 || quota.ReplicaUsed != 4 {
			t.Fatalf("failed resize target quota not reserved: %+v", quota)
		}
		if err := coordinator.ReconcileOne(ctx, failedState.Workflow.ID); err != nil {
			t.Fatalf("failing resize network step: %v", err)
		}
		if err := coordinator.ReconcileOne(ctx, failedState.Workflow.ID); err == nil {
			t.Fatal("expected injected compute resize failure")
		}
		failedState, _ = repo.GetWorkflowState(ctx, failedState.Workflow.ID)
		if failedState.Workflow.Status != domain.CloudWorkflowCompensating {
			t.Fatalf("failed resize status=%s want compensating", failedState.Workflow.Status)
		}
		for i := 0; i < 3; i++ {
			if err := coordinator.ReconcileOne(ctx, failedState.Workflow.ID); err != nil {
				t.Fatalf("resize compensation tick %d: %v", i, err)
			}
		}
		failedState, err = repo.GetWorkflowState(ctx, failedState.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if failedState.Workflow.Status != domain.CloudWorkflowCompensated || failedState.Resource.ActualState != domain.CloudResourceActive {
			t.Fatalf("resize compensation did not restore active resource: workflow=%s resource=%s", failedState.Workflow.Status, failedState.Resource.ActualState)
		}
		quota = mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 1800 || quota.MemoryMiBUsed != 1023 || quota.ReplicaUsed != 3 {
			t.Fatalf("resize compensation did not restore prior quota: %+v", quota)
		}

		// 4) Delete is fail-forward. A storage-delete failure must remain in the
		// forward workflow and retry; it must never recreate the deleted compute.
		provider.failDeleteStorageOnce = true
		provider.mu.Lock()
		computeCallsBeforeDelete := provider.ensureComputeCalls
		provider.mu.Unlock()
		deleteState, err := coordinator.CreateDeleteWorkflow(ctx, resourceID, "saga-life-delete-"+suffix)
		if err != nil {
			t.Fatalf("create delete workflow: %v", err)
		}
		if err := coordinator.ReconcileOne(ctx, deleteState.Workflow.ID); err != nil {
			t.Fatalf("delete compute: %v", err)
		}
		if err := coordinator.ReconcileOne(ctx, deleteState.Workflow.ID); err == nil {
			t.Fatal("expected first storage delete attempt to fail")
		}
		deleteState, _ = repo.GetWorkflowState(ctx, deleteState.Workflow.ID)
		if deleteState.Workflow.Status != domain.CloudWorkflowRunning {
			t.Fatalf("delete failure entered wrong state: %s", deleteState.Workflow.Status)
		}
		assertSagaStepStatus(t, deleteState, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepRunning)
		quota = mustServiceQuota(t, repo, ctx, tenant)
		if quota.ReplicaUsed != 3 {
			t.Fatalf("delete released quota before external cleanup: %+v", quota)
		}
		// Retry storage, then network, quota and commit.
		for i := 0; i < 4; i++ {
			if err := coordinator.ReconcileOne(ctx, deleteState.Workflow.ID); err != nil {
				t.Fatalf("delete retry tick %d: %v", i, err)
			}
		}
		deleteState, err = repo.GetWorkflowState(ctx, deleteState.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if deleteState.Workflow.Status != domain.CloudWorkflowSucceeded || deleteState.Order.Status != domain.CloudOrderSucceeded || deleteState.Resource.ActualState != domain.CloudResourceDeleted {
			t.Fatalf("delete did not converge: workflow=%s order=%s resource=%s", deleteState.Workflow.Status, deleteState.Order.Status, deleteState.Resource.ActualState)
		}
		quota = mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 0 || quota.MemoryMiBUsed != 0 || quota.ReplicaUsed != 0 {
			t.Fatalf("delete did not release quota after cleanup: %+v", quota)
		}
		provider.mu.Lock()
		computeCallsAfterDelete := provider.ensureComputeCalls
		storageDeleteAttempts := provider.deleteStorageAttempts
		provider.mu.Unlock()
		if computeCallsAfterDelete != computeCallsBeforeDelete {
			t.Fatalf("delete fail-forward unexpectedly recreated compute: before=%d after=%d", computeCallsBeforeDelete, computeCallsAfterDelete)
		}
		if storageDeleteAttempts != 2 {
			t.Fatalf("storage delete attempts=%d want=2", storageDeleteAttempts)
		}
		history, err := repo.ListWorkflowsByResource(ctx, resourceID, 20, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 4 {
			t.Fatalf("workflow history count=%d want=4", len(history))
		}
		wantKinds := []string{domain.CloudWorkflowKindDelete, domain.CloudWorkflowKindResize, domain.CloudWorkflowKindResize, domain.CloudWorkflowKindProvision}
		for i, want := range wantKinds {
			if history[i].Kind != want {
				t.Fatalf("workflow history[%d].kind=%s want=%s", i, history[i].Kind, want)
			}
		}
	})

	t.Run("bucketed saga compensation releases the exact reservation bucket", func(t *testing.T) {
		if err := repo.SetQuotaBucketCount(4); err != nil {
			t.Fatal(err)
		}
		if err := repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionBucketed); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionLocked)
			_ = repo.SetQuotaBucketCount(16)
		}()

		tenant := "saga-bucket-comp-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 4000, MemoryMiB: 4096, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{failForwardStep: domain.CloudWorkflowStepStorage}
		coordinator := NewCloudWorkflowService(repo, provider)
		state, err := coordinator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "bucket-comp-"+suffix), "saga-bucket-comp-key-"+suffix)
		if err != nil {
			t.Fatal(err)
		}
		if state.Resource.QuotaBucketID == 0 {
			t.Fatal("bucketed Saga admission did not persist quota_bucket_id")
		}
		bucketID := state.Resource.QuotaBucketID
		var before domain.CloudTenantQuotaBucket
		if err := db.First(&before, "tenant_id = ? AND bucket_id = ?", tenant, bucketID).Error; err != nil {
			t.Fatal(err)
		}
		if before.CPUMilliUsed != 1000 || before.MemoryMiBUsed != 512 || before.ReplicaUsed != 2 {
			t.Fatalf("reservation bucket usage=%+v", before)
		}

		if err := coordinator.ReconcileOne(ctx, state.Workflow.ID); err != nil {
			t.Fatalf("network forward: %v", err)
		}
		if err := coordinator.ReconcileOne(ctx, state.Workflow.ID); err == nil {
			t.Fatal("expected injected storage failure")
		}
		for i := 0; i < 6; i++ {
			state, err = repo.GetWorkflowState(ctx, state.Workflow.ID)
			if err != nil {
				t.Fatal(err)
			}
			if state.Workflow.Status == domain.CloudWorkflowCompensated {
				break
			}
			if err := coordinator.ReconcileOne(ctx, state.Workflow.ID); err != nil {
				t.Fatalf("compensation tick %d: %v", i, err)
			}
		}
		state, err = repo.GetWorkflowState(ctx, state.Workflow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Workflow.Status != domain.CloudWorkflowCompensated || state.Resource.ActualState != domain.CloudResourceDeleted {
			t.Fatalf("bucketed compensation terminal state workflow=%s resource=%s", state.Workflow.Status, state.Resource.ActualState)
		}
		if state.Resource.QuotaBucketID != bucketID {
			t.Fatalf("quota bucket changed during compensation before=%d after=%d", bucketID, state.Resource.QuotaBucketID)
		}
		var after domain.CloudTenantQuotaBucket
		if err := db.First(&after, "tenant_id = ? AND bucket_id = ?", tenant, bucketID).Error; err != nil {
			t.Fatal(err)
		}
		if after.CPUMilliUsed != 0 || after.MemoryMiBUsed != 0 || after.ReplicaUsed != 0 {
			t.Fatalf("compensation did not release exact bucket: before=%+v after=%+v", before, after)
		}
		quota := mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 0 || quota.MemoryMiBUsed != 0 || quota.ReplicaUsed != 0 {
			t.Fatalf("aggregate bucketed quota not released: %+v", quota)
		}
	})

	t.Run("forward failure compensates in reverse and retries compensation", func(t *testing.T) {
		tenant := "saga-fail-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 4000, MemoryMiB: 4096, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{failForwardStep: domain.CloudWorkflowStepStorage, failDeleteNetworkOnce: true}
		coordinator := NewCloudWorkflowService(repo, provider)
		state, err := coordinator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "bundle-fail-"+suffix), "saga-fail-key-"+suffix)
		if err != nil {
			t.Fatalf("create failing workflow: %v", err)
		}
		workflowID := state.Workflow.ID

		if err := coordinator.ReconcileOne(ctx, workflowID); err != nil {
			t.Fatalf("network forward: %v", err)
		}
		if err := coordinator.ReconcileOne(ctx, workflowID); err == nil {
			t.Fatal("expected injected storage failure")
		}
		state, _ = repo.GetWorkflowState(ctx, workflowID)
		if state.Workflow.Status != domain.CloudWorkflowCompensating {
			t.Fatalf("workflow status=%s want compensating", state.Workflow.Status)
		}
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepSucceeded)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepFailed)

		// The failed storage step is compensated first because an API error may
		// be ambiguous: the PVC might have been created before the response was
		// lost. Delete is idempotent, so compensating the failed step is safer.
		if err := coordinator.ReconcileOne(ctx, workflowID); err != nil {
			t.Fatalf("storage compensation: %v", err)
		}
		state, _ = repo.GetWorkflowState(ctx, workflowID)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepCompensated)

		// Network compensation fails once. The workflow must remain durable and
		// keep quota charged while the network side effect may still exist.
		if err := coordinator.ReconcileOne(ctx, workflowID); err == nil {
			t.Fatal("expected injected network compensation failure")
		}
		state, _ = repo.GetWorkflowState(ctx, workflowID)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepCompensating)
		quota := mustServiceQuota(t, repo, ctx, tenant)
		if quota.ReplicaUsed != 2 {
			t.Fatalf("quota released before compensation completed: %+v", quota)
		}

		// New coordinator instance retries the persisted compensating step, then
		// the following tick releases quota and finalizes the failed create.
		restarted := NewCloudWorkflowService(repo, provider)
		if err := restarted.ReconcileOne(ctx, workflowID); err != nil {
			t.Fatalf("retry network compensation: %v", err)
		}
		if err := restarted.ReconcileOne(ctx, workflowID); err != nil {
			t.Fatalf("quota compensation: %v", err)
		}
		state, err = repo.GetWorkflowState(ctx, workflowID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Workflow.Status != domain.CloudWorkflowCompensated || state.Order.Status != domain.CloudOrderFailed || state.Resource.ActualState != domain.CloudResourceDeleted {
			t.Fatalf("unexpected compensated terminal state: workflow=%s order=%s resource=%s", state.Workflow.Status, state.Order.Status, state.Resource.ActualState)
		}
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepQuota, domain.CloudWorkflowStepCompensated)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepCompensated)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepCompensated)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepCompute, domain.CloudWorkflowStepSkipped)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepCommit, domain.CloudWorkflowStepSkipped)
		quota = mustServiceQuota(t, repo, ctx, tenant)
		if quota.CPUMilliUsed != 0 || quota.MemoryMiBUsed != 0 || quota.ReplicaUsed != 0 {
			t.Fatalf("quota not released after complete compensation: %+v", quota)
		}
	})

	t.Run("optimized batch prefetch and fused writes converge", func(t *testing.T) {
		if err := repo.SetWorkflowDBMode(repository.CloudWorkflowDBOptimized); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = repo.SetWorkflowDBMode(repository.CloudWorkflowDBBaseline) }()
		if err := repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionAtomicVersioned); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionLocked) }()

		tenant := "saga-optimized-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 4000, MemoryMiB: 4096, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		coordinator := NewCloudWorkflowService(repo, provider)
		state, err := coordinator.CreateProvisionWorkflow(ctx, sagaWorkflowInput(tenant, "optimized-"+suffix), "saga-optimized-"+suffix)
		if err != nil {
			t.Fatalf("create optimized workflow: %v", err)
		}
		for i := 0; i < 10 && state.Workflow.Status != domain.CloudWorkflowSucceeded; i++ {
			if _, err := coordinator.ReconcilePending(ctx, 50); err != nil {
				t.Fatalf("optimized reconcile pass %d: %v", i, err)
			}
			state, err = repo.GetWorkflowState(ctx, state.Workflow.ID)
			if err != nil {
				t.Fatal(err)
			}
		}
		if state.Workflow.Status != domain.CloudWorkflowSucceeded || state.Order.Status != domain.CloudOrderSucceeded || state.Resource.ActualState != domain.CloudResourceActive {
			t.Fatalf("optimized terminal state workflow=%s order=%s resource=%s", state.Workflow.Status, state.Order.Status, state.Resource.ActualState)
		}
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepNetwork, domain.CloudWorkflowStepSucceeded)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepStorage, domain.CloudWorkflowStepSucceeded)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepCompute, domain.CloudWorkflowStepSucceeded)
		assertSagaStepStatus(t, state, domain.CloudWorkflowStepCommit, domain.CloudWorkflowStepSucceeded)
	})

	t.Run("optimized worker pool converges a claimed batch", func(t *testing.T) {
		if err := repo.SetWorkflowDBMode(repository.CloudWorkflowDBOptimized); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = repo.SetWorkflowDBMode(repository.CloudWorkflowDBBaseline) }()
		if err := repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionAtomicVersioned); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = repo.SetQuotaAdmissionMode(repository.CloudQuotaAdmissionLocked) }()

		tenant := "saga-worker-pool-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 16000, MemoryMiB: 16384, Replicas: 32}); err != nil {
			t.Fatal(err)
		}
		provider := &sagaFakeProvider{}
		coordinator := NewCloudWorkflowService(repo, provider)
		coordinator.SetWorkerConcurrency(4)
		workflowIDs := make([]uint64, 0, 8)
		for i := 0; i < 8; i++ {
			state, err := coordinator.CreateProvisionWorkflow(
				ctx,
				sagaWorkflowInput(tenant, fmt.Sprintf("worker-%02d-%s", i, suffix)),
				fmt.Sprintf("saga-worker-%02d-%s", i, suffix),
			)
			if err != nil {
				t.Fatalf("create worker workflow %d: %v", i, err)
			}
			workflowIDs = append(workflowIDs, state.Workflow.ID)
		}

		for pass := 0; pass < 12; pass++ {
			if _, err := coordinator.ReconcilePending(ctx, 50); err != nil {
				t.Fatalf("worker reconcile pass %d: %v", pass, err)
			}
			terminal := 0
			for _, workflowID := range workflowIDs {
				state, err := repo.GetWorkflowState(ctx, workflowID)
				if err != nil {
					t.Fatal(err)
				}
				if state.Workflow.Status == domain.CloudWorkflowSucceeded {
					terminal++
				}
			}
			if terminal == len(workflowIDs) {
				break
			}
		}

		for _, workflowID := range workflowIDs {
			state, err := repo.GetWorkflowState(ctx, workflowID)
			if err != nil {
				t.Fatal(err)
			}
			if state.Workflow.Status != domain.CloudWorkflowSucceeded || state.Order.Status != domain.CloudOrderSucceeded || state.Resource.ActualState != domain.CloudResourceActive {
				t.Fatalf("worker workflow=%d terminal workflow=%s order=%s resource=%s", workflowID, state.Workflow.Status, state.Order.Status, state.Resource.ActualState)
			}
		}
	})
}

func sagaWorkflowInput(tenant, name string) CreateProvisionWorkflowInput {
	return CreateProvisionWorkflowInput{
		TenantID: tenant,
		Name:     name,
		Provider: "kubernetes",
		Spec: cloudcontrol.KubernetesBundleSpec{
			Compute: cloudcontrol.KubernetesDeploymentSpec{
				Namespace:     "default",
				Image:         "nginx:1.27",
				Replicas:      2,
				CPU:           "500m",
				Memory:        "256Mi",
				ContainerPort: 8080,
			},
			Network: cloudcontrol.KubernetesNetworkSpec{Enabled: true, Port: 80, TargetPort: 8080},
			Storage: cloudcontrol.KubernetesStorageSpec{Enabled: true, Size: "1Gi", MountPath: "/data"},
		},
	}
}

func resizeSagaWorkflowInput(replicas int32, cpu, memory string, servicePort int32) ResizeBundleWorkflowInput {
	return ResizeBundleWorkflowInput{
		Spec: cloudcontrol.KubernetesBundleSpec{
			Compute: cloudcontrol.KubernetesDeploymentSpec{
				Namespace:     "default",
				Image:         "nginx:1.27",
				Replicas:      replicas,
				CPU:           cpu,
				Memory:        memory,
				ContainerPort: 8080,
			},
			Network: cloudcontrol.KubernetesNetworkSpec{Enabled: true, Port: servicePort, TargetPort: 8080},
			Storage: cloudcontrol.KubernetesStorageSpec{Enabled: true, Size: "1Gi", MountPath: "/data"},
		},
	}
}

func assertSagaStepStatus(t *testing.T, state *repository.CloudWorkflowState, name string, want domain.CloudWorkflowStepStatus) {
	t.Helper()
	for _, step := range state.Steps {
		if step.Name == name {
			if step.Status != want {
				t.Fatalf("step %s status=%s want=%s", name, step.Status, want)
			}
			return
		}
	}
	t.Fatalf("step %s not found", name)
}

func mustServiceQuota(t *testing.T, repo *repository.CloudResourceRepository, ctx context.Context, tenant string) *domain.CloudTenantQuota {
	t.Helper()
	quota, err := repo.GetTenantQuota(ctx, tenant)
	if err != nil {
		t.Fatalf("get quota: %v", err)
	}
	return quota
}
