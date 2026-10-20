package repository

import (
	"os"
	"strings"
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/testsupport"
)

// This migration test must be run against a fresh disposable MySQL schema.
// It deliberately drops and rebuilds a workflow index; do not run on live DB.
func TestManagedWorkflowSchemaMySQL(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MANAGED_WORKFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("requires fresh TASKFLOW_MANAGED_WORKFLOW_TEST_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("refusing non-test MySQL schema")
	}
	db, err := NewMySQL(dsn, 8, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	r := NewTaskRepository(db)
	r.ConfigureManagedWorkflowSchema(true)
	if err := r.Migrate(); err == nil || !strings.Contains(err.Error(), "run schema-migrate") {
		t.Fatalf("unmigrated managed mode must fail closed, got %v", err)
	}
	if err := r.MigrateWorkflowSchemaExplicit(); err != nil {
		t.Fatalf("explicit migration must succeed: %v", err)
	}
	if err := r.Migrate(); err != nil {
		t.Fatalf("server startup should accept managed workflow schema: %v", err)
	}
	if err := db.Exec("ALTER TABLE cloud_workflows ADD UNIQUE KEY ux_cloud_workflow_resource (resource_id)").Error; err != nil {
		t.Fatalf("seed historic unique index: %v", err)
	}
	if err := r.Migrate(); err == nil || !strings.Contains(err.Error(), "legacy one-workflow") {
		t.Fatalf("managed server should not silently drop legacy index: %v", err)
	}
	if err := r.MigrateWorkflowSchemaExplicit(); err != nil {
		t.Fatalf("explicit rerun should normalize historical index: %v", err)
	}
	if err := r.Migrate(); err != nil {
		t.Fatalf("managed startup after index repair: %v", err)
	}
	if err := db.Migrator().DropIndex(&domain.CloudWorkflow{}, "idx_cloud_workflow_runnable"); err != nil {
		t.Fatalf("drop index in disposable test: %v", err)
	}
	if err := r.Migrate(); err == nil || !strings.Contains(err.Error(), "missing index") {
		t.Fatalf("managed server should not quietly rebuild missing index: %v", err)
	}
	if err := r.MigrateWorkflowSchemaExplicit(); err != nil {
		t.Fatalf("operator-managed recovery: %v", err)
	}
	if err := r.VerifyManagedWorkflowSchema(); err != nil {
		t.Fatalf("versioned managed schema not ready: %v", err)
	}
}
