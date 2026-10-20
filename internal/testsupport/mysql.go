// Package testsupport contains safety preflight checks for destructive tests.
package testsupport

import (
	"strings"

	"github.com/go-sql-driver/mysql"
)

// DisposableMySQLDSN rejects an integration DSN that might point to a live
// application schema. Dedicated test databases must have an explicit *_ci,
// *_test or test_* name. This is a guardrail, not a substitute for separate
// database users and network isolation.
func DisposableMySQLDSN(raw string) bool {
	config, err := mysql.ParseDSN(raw)
	if err != nil {
		return false
	}
	database := strings.ToLower(strings.TrimSpace(config.DBName))
	if database == "" {
		return false
	}
	return strings.HasSuffix(database, "_ci") || strings.HasSuffix(database, "_test") || strings.HasPrefix(database, "test_")
}
