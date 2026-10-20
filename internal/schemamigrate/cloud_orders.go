// Package schemamigrate contains explicit, versioned MySQL schema transitions.
// Unlike GORM AutoMigrate, destructive changes require an operator action.
package schemamigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	compositeIndex = "idx_cloud_orders_tenant_idempotency"
	lockName       = "taskflow:schema:migrate"
	versionExpand  = "20261009_cloud_order_tenant_idempotency_expand"
	versionCutover = "20261009_cloud_order_tenant_idempotency_contract"
)

type Index struct {
	Name    string
	Unique  bool
	Columns []string
}

type Status struct {
	CompositeReady bool
	LegacyUnique   []string
	Applied        []string
}

// Inspect performs only reads and is safe against a running production DB.
func Inspect(ctx context.Context, db *sql.DB) (Status, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return Status{}, err
	}
	defer conn.Close()
	return inspect(ctx, conn)
}

// Apply changes only the cloud_orders index. phase=expand safely adds the
// composite index while keeping the global constraint. phase=contract drops
// only the legacy global uniqueness index, and is forbidden until every old
// application writer is fully drained.
func Apply(ctx context.Context, db *sql.DB, phase string, oldWritersStopped bool) (Status, error) {
	if phase != "expand" && phase != "contract" {
		return Status{}, fmt.Errorf("unsupported schema migration phase %q", phase)
	}
	if phase == "contract" && !oldWritersStopped {
		return Status{}, errors.New("contract requires explicit acknowledgement that all legacy writers have stopped")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return Status{}, err
	}
	defer conn.Close()
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", lockName).Scan(&acquired); err != nil {
		return Status{}, fmt.Errorf("acquire schema lock: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return Status{}, errors.New("schema migration lock busy")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released sql.NullInt64
		_ = conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS taskflow_schema_migrations (
		version VARCHAR(128) NOT NULL PRIMARY KEY,
		description VARCHAR(255) NOT NULL,
		applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	) ENGINE=InnoDB`); err != nil {
		return Status{}, fmt.Errorf("initialize migration journal: %w", err)
	}

	status, err := inspect(ctx, conn)
	if err != nil {
		return Status{}, err
	}
	if !status.CompositeReady {
		if _, err := conn.ExecContext(ctx, "ALTER TABLE cloud_orders ADD UNIQUE KEY `"+compositeIndex+"` (`tenant_id`, `idempotency_key`)"); err != nil {
			return Status{}, fmt.Errorf("expand tenant idempotency uniqueness: %w", err)
		}
	}
	if err := recordMigration(ctx, conn, versionExpand, "retain old unique index; create tenant-scoped unique index"); err != nil {
		return Status{}, err
	}

	if phase == "contract" {
		status, err = inspect(ctx, conn)
		if err != nil {
			return Status{}, err
		}
		if !status.CompositeReady {
			return Status{}, errors.New("cannot contract without composite unique index")
		}
		for _, name := range status.LegacyUnique {
			// The name is read from information_schema; quote even the embedded
			// backtick rather than interpolating an untrusted identifier raw.
			quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
			if _, err := conn.ExecContext(ctx, "ALTER TABLE cloud_orders DROP INDEX "+quoted); err != nil {
				return Status{}, fmt.Errorf("drop legacy index %q: %w", name, err)
			}
		}
		if err := recordMigration(ctx, conn, versionCutover, "old writers stopped; remove global idempotency uniqueness"); err != nil {
			return Status{}, err
		}
	}
	return inspect(ctx, conn)
}

func recordMigration(ctx context.Context, conn *sql.Conn, version, description string) error {
	_, err := conn.ExecContext(ctx, "INSERT INTO taskflow_schema_migrations (version, description) VALUES (?, ?) ON DUPLICATE KEY UPDATE version = version", version, description)
	return err
}

func inspect(ctx context.Context, conn *sql.Conn) (Status, error) {
	rows, err := conn.QueryContext(ctx, `SELECT index_name, non_unique, seq_in_index, column_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'cloud_orders'
		ORDER BY index_name, seq_in_index`)
	if err != nil {
		return Status{}, err
	}
	indexes := make(map[string]*Index)
	for rows.Next() {
		var name, column string
		var nonUnique, order int
		if err := rows.Scan(&name, &nonUnique, &order, &column); err != nil {
			rows.Close()
			return Status{}, err
		}
		idx := indexes[name]
		if idx == nil {
			idx = &Index{Name: name, Unique: nonUnique == 0}
			indexes[name] = idx
		}
		idx.Columns = append(idx.Columns, column)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Status{}, err
	}
	rows.Close()
	if len(indexes) == 0 {
		return Status{}, errors.New("cloud_orders table does not exist or has no indexes; initialize schema first")
	}
	var status Status
	for _, idx := range indexes {
		if !idx.Unique {
			continue
		}
		if idx.Name == compositeIndex && len(idx.Columns) == 2 && idx.Columns[0] == "tenant_id" && idx.Columns[1] == "idempotency_key" {
			status.CompositeReady = true
		}
		if len(idx.Columns) == 1 && idx.Columns[0] == "idempotency_key" {
			status.LegacyUnique = append(status.LegacyUnique, idx.Name)
		}
	}
	var tableCount int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'taskflow_schema_migrations'`).Scan(&tableCount); err != nil {
		return Status{}, err
	}
	if tableCount > 0 {
		versionRows, err := conn.QueryContext(ctx, "SELECT version FROM taskflow_schema_migrations ORDER BY version")
		if err != nil {
			return Status{}, err
		}
		for versionRows.Next() {
			var version string
			if err := versionRows.Scan(&version); err != nil {
				versionRows.Close()
				return Status{}, err
			}
			status.Applied = append(status.Applied, version)
		}
		if err := versionRows.Err(); err != nil {
			versionRows.Close()
			return Status{}, err
		}
		versionRows.Close()
	}
	return status, nil
}
