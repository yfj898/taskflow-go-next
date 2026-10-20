package schemamigrate

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/testsupport"
)

// Use a separate, disposable database: the test intentionally drops a
// legacy uniqueness index, and MUST NOT target production or shared staging.
func TestCloudOrdersExpandContractMySQL(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_SCHEMA_MIGRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("requires disposable TASKFLOW_SCHEMA_MIGRATION_TEST_DSN")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("unsafe migration-test DSN: database must be an explicit *_ci or *_test schema")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = db.ExecContext(ctx, `CREATE TABLE cloud_orders (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		tenant_id VARCHAR(128) NOT NULL,
		idempotency_key VARCHAR(128) NOT NULL,
		UNIQUE KEY idx_cloud_orders_idempotency_key (idempotency_key)
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO cloud_orders (tenant_id, idempotency_key) VALUES ('tenant-a', 'same-key')"); err != nil {
		t.Fatal(err)
	}
	before, err := Inspect(ctx, db)
	if err != nil || before.CompositeReady || len(before.LegacyUnique) != 1 {
		t.Fatalf("legacy state: %+v, %v", before, err)
	}
	if _, err := Apply(ctx, db, "contract", false); err == nil || !strings.Contains(err.Error(), "legacy writers") {
		t.Fatalf("contract without approval must fail: %v", err)
	}
	expanded, err := Apply(ctx, db, "expand", false)
	if err != nil || !expanded.CompositeReady || len(expanded.LegacyUnique) != 1 {
		t.Fatalf("expand must keep old index: %+v err=%v", expanded, err)
	}
	appDB, err := repository.NewMySQL(dsn, 8, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	appSQLDB, err := appDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer appSQLDB.Close()
	if err := repository.NewTaskRepository(appDB).Migrate(); err != nil {
		t.Fatalf("startup migration against legacy index: %v", err)
	}
	if status, err := Inspect(ctx, db); err != nil || len(status.LegacyUnique) != 1 {
		t.Fatalf("startup must preserve old unique index: %+v err=%v", status, err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO cloud_orders (tenant_id, idempotency_key) VALUES ('tenant-b', 'same-key')"); err == nil {
		t.Fatal("duplicate tenant key succeeded before old writers stopped")
	}
	if _, err := Apply(ctx, db, "expand", false); err != nil {
		t.Fatalf("expand rerun not idempotent: %v", err)
	}
	contracted, err := Apply(ctx, db, "contract", true)
	if err != nil || !contracted.CompositeReady || len(contracted.LegacyUnique) != 0 || len(contracted.Applied) != 2 {
		t.Fatalf("contract state: %+v err=%v", contracted, err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO cloud_orders (tenant_id, idempotency_key) VALUES ('tenant-b', 'same-key')"); err != nil {
		t.Fatalf("same key must be allowed for different tenants: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO cloud_orders (tenant_id, idempotency_key) VALUES ('tenant-a', 'same-key')"); err == nil {
		t.Fatal("duplicate key accepted within one tenant")
	}
	if err := repository.NewTaskRepository(appDB).Migrate(); err != nil {
		t.Fatalf("startup migration against contract schema: %v", err)
	}
	if status, err := Inspect(ctx, db); err != nil || len(status.LegacyUnique) != 0 {
		t.Fatalf("startup must not restore legacy index after contract: %+v err=%v", status, err)
	}
	if status, err := Apply(ctx, db, "contract", true); err != nil || len(status.LegacyUnique) != 0 {
		t.Fatalf("contract rerun not idempotent: %+v %v", status, err)
	}
}
