package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewMySQL(dsn string, maxOpenConns, maxIdleConns int, skipDefaultTx bool) (*gorm.DB, error) {
	return openMySQL(dsn, maxOpenConns, maxIdleConns, skipDefaultTx)
}

func NewMySQLWithFailover(primaryDSN, failoverDSNs string, maxOpenConns, maxIdleConns int, skipDefaultTx bool, connectTimeout time.Duration) (*gorm.DB, int, error) {
	candidates := []string{strings.TrimSpace(primaryDSN)}
	for _, dsn := range strings.Split(failoverDSNs, ";") {
		if dsn = strings.TrimSpace(dsn); dsn != "" {
			candidates = append(candidates, dsn)
		}
	}
	if connectTimeout <= 0 {
		connectTimeout = 3 * time.Second
	}
	var failures []string
	for index, dsn := range candidates {
		if dsn == "" {
			continue
		}
		db, err := openMySQL(dsn, maxOpenConns, maxIdleConns, skipDefaultTx)
		if err != nil {
			failures = append(failures, fmt.Sprintf("candidate=%d open=%v", index, err))
			continue
		}
		sqlDB, err := db.DB()
		if err != nil {
			failures = append(failures, fmt.Sprintf("candidate=%d sql-db=%v", index, err))
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
		err = sqlDB.PingContext(ctx)
		cancel()
		if err == nil {
			return db, index, nil
		}
		_ = sqlDB.Close()
		failures = append(failures, fmt.Sprintf("candidate=%d ping=%v", index, err))
	}
	return nil, -1, fmt.Errorf("all MySQL writer candidates failed: %s", strings.Join(failures, "; "))
}

func openMySQL(dsn string, maxOpenConns, maxIdleConns int, skipDefaultTx bool) (*gorm.DB, error) {
	// TaskFlow explicitly opens a transaction where atomicity matters
	// (task + outbox). The remaining writes are single SQL statements, so
	// GORM's implicit transaction around every create/update only adds an
	// extra BEGIN/COMMIT round trip without improving correctness.
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Warn),
		SkipDefaultTransaction: skipDefaultTx,
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	if maxOpenConns <= 0 {
		maxOpenConns = 50
	}
	if maxIdleConns <= 0 || maxIdleConns > maxOpenConns {
		maxIdleConns = 10
		if maxIdleConns > maxOpenConns {
			maxIdleConns = maxOpenConns
		}
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}
