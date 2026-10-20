package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/testsupport"
)

// This integration test runs against an explicit disposable MySQL database.
// It never runs against the production DSN unless someone explicitly provides
// that DSN as TASKFLOW_MYSQL_INTEGRATION_DSN (do not do this).
func TestCloudTenantScopeMySQLIntegration(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_MYSQL_INTEGRATION_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("unsafe integration DSN: database must be an explicit *_ci or *_test schema")
	}
	db, err := NewMySQL(dsn, 12, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.CloudResource{}, &domain.CloudOrder{}, &domain.CloudWorkflow{}, &domain.CloudWorkflowStep{}, &domain.CloudTenantQuota{}, &domain.CloudTenantQuotaBucket{}); err != nil {
		t.Fatal(err)
	}

	repo := NewCloudResourceRepository(db)
	ctx := context.Background()
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	teamA, teamB := "team-a-"+suffix, "team-b-"+suffix
	for _, tenant := range []string{teamA, teamB} {
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 10000, MemoryMiB: 10240, Replicas: 10}); err != nil {
			t.Fatal(err)
		}
	}
	resourceA := testCloudResource(teamA, "app-a-"+suffix, `{"image":"nginx:1.27","replicas":1,"cpu":"200m","memory":"256Mi"}`)
	resourceB := testCloudResource(teamB, "app-b-"+suffix, `{"image":"nginx:1.27","replicas":1,"cpu":"200m","memory":"256Mi"}`)
	orderA := testCloudOrder(teamA, domain.CloudOrderCreate, "key-a-"+suffix)
	orderB := testCloudOrder(teamB, domain.CloudOrderCreate, "key-b-"+suffix)
	for i, resource := range []*domain.CloudResource{resourceA, resourceB} {
		order := []*domain.CloudOrder{orderA, orderB}[i]
		if err := repo.CreateResourceWithOrder(ctx, resource, order, cloudcontrol.QuotaDemand{CPUMilli: 200, MemoryMiB: 256, Replicas: 1}); err != nil {
			t.Fatal(err)
		}
	}

	aCtx := WithCloudTenantGrants(ctx, []string{teamA})
	bCtx := WithCloudTenantGrants(ctx, []string{teamB})
	denyCtx := WithCloudTenantGrants(ctx, nil)
	if got, err := repo.GetResource(aCtx, resourceA.ID); err != nil || got.TenantID != teamA {
		t.Fatalf("own resource read: got=%+v err=%v", got, err)
	}
	if _, err := repo.GetResource(aCtx, resourceB.ID); !errors.Is(err, ErrCloudResourceNotFound) {
		t.Fatalf("foreign resource ID must not be readable, err=%v", err)
	}
	if _, err := repo.GetResource(denyCtx, resourceA.ID); !errors.Is(err, ErrCloudResourceNotFound) {
		t.Fatalf("empty tenant grant must fail closed, err=%v", err)
	}
	if _, err := repo.GetResource(bCtx, resourceB.ID); err != nil {
		t.Fatalf("team B access lost: %v", err)
	}
	if _, err := repo.FindOrderByIdempotencyKey(aCtx, orderB.IdempotencyKey); !errors.Is(err, ErrCloudOrderNotFound) {
		t.Fatalf("foreign idempotency record leaked: %v", err)
	}
	if got, err := repo.ListResources(aCtx, "", 100, 0); err != nil || len(got) != 1 || got[0].ID != resourceA.ID {
		t.Fatalf("cross-tenant resource list leak: %+v err=%v", got, err)
	}
	if got, err := repo.ListResources(aCtx, teamB, 100, 0); err != nil || len(got) != 0 {
		t.Fatalf("foreign tenant resource list leak: %+v err=%v", got, err)
	}
	if got, err := repo.ListOrders(aCtx, resourceB.ID, 100, 0); err != nil || len(got) != 0 {
		t.Fatalf("foreign tenant orders leak: %+v err=%v", got, err)
	}
	if _, err := repo.SetTenantQuota(aCtx, teamB, cloudcontrol.QuotaDemand{CPUMilli: 1}); !errors.Is(err, ErrCloudTenantAccessDenied) {
		t.Fatalf("foreign quota modification accepted: %v", err)
	}
	if _, err := repo.GetTenantQuota(aCtx, teamB); !errors.Is(err, ErrCloudTenantAccessDenied) {
		t.Fatalf("foreign quota read accepted: %v", err)
	}
	if _, err := repo.ResizeResourceWithOrder(aCtx, resourceB.ID, resourceB.DesiredSpec, cloudcontrol.QuotaDemand{CPUMilli: 400, MemoryMiB: 512, Replicas: 2}, testCloudOrder(teamB, domain.CloudOrderResize, "cross-resize-"+suffix)); !errors.Is(err, ErrCloudResourceNotFound) {
		t.Fatalf("foreign resize accepted: %v", err)
	}
	if _, err := repo.DeleteResourceWithOrder(aCtx, resourceB.ID, testCloudOrder(teamB, domain.CloudOrderDelete, "cross-delete-"+suffix)); !errors.Is(err, ErrCloudResourceNotFound) {
		t.Fatalf("foreign delete accepted: %v", err)
	}
	if _, err := repo.DeleteResourceWithOrder(bCtx, resourceB.ID, testCloudOrder(teamA, domain.CloudOrderDelete, "misowned-"+suffix)); !errors.Is(err, ErrCloudTenantAccessDenied) {
		t.Fatalf("mismatched order tenant accepted: %v", err)
	}
	if got, err := repo.GetResource(ctx, resourceB.ID); err != nil || got.Generation != 1 {
		t.Fatalf("foreign attempted write changed resource: %+v err=%v", got, err)
	}

	workflowB := &domain.CloudWorkflow{
		ResourceID: resourceB.ID, OrderID: orderB.ID, TenantID: teamB,
		Kind: domain.CloudWorkflowKindProvision, Status: domain.CloudWorkflowPending, Spec: "{}",
	}
	if err := db.Create(workflowB).Error; err != nil {
		t.Fatalf("seed workflow: %v", err)
	}
	if _, err := repo.GetWorkflowState(aCtx, workflowB.ID); !errors.Is(err, ErrCloudWorkflowNotFound) {
		t.Fatalf("foreign workflow ID must not be readable: %v", err)
	}
	if _, err := repo.FindWorkflowByOrderID(aCtx, orderB.ID); !errors.Is(err, ErrCloudWorkflowNotFound) {
		t.Fatalf("foreign workflow by order leaked: %v", err)
	}
	if got, err := repo.ListWorkflowsByResource(aCtx, resourceB.ID, 100, 0); err != nil || len(got) != 0 {
		t.Fatalf("foreign workflow list leak: %+v err=%v", got, err)
	}
	if got, err := repo.GetWorkflowState(bCtx, workflowB.ID); err != nil || got.Resource.TenantID != teamB {
		t.Fatalf("own workflow read blocked: %+v err=%v", got, err)
	}
}
