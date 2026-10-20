package testsupport

import "testing"

func TestDisposableMySQLDSN(t *testing.T) {
	for _, tc := range []struct {
		name, dsn string
		want      bool
	}{
		{"tenant_ci", "root:pass@tcp(127.0.0.1:3306)/taskflow_tenant_ci?parseTime=true", true},
		{"schema_test", "root:pass@tcp(127.0.0.1:3306)/cloud_schema_test?parseTime=true", true},
		{"prefix_test", "root:pass@tcp(127.0.0.1:3306)/test_cloud?parseTime=true", true},
		{"reject_production", "root:pass@tcp(prod:3306)/taskflow?parseTime=true", false},
		{"reject_missing_schema", "root:pass@tcp(prod:3306)/", false},
		{"reject_malformed", "not a mysql DSN", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DisposableMySQLDSN(tc.dsn); got != tc.want {
				t.Fatalf("DisposableMySQLDSN() = %t, want %t", got, tc.want)
			}
		})
	}
}
