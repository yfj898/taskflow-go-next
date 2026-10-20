package config

import (
	"strings"
	"testing"
)

func TestCloudTenantAuthzRequiresVerifiedOIDC(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cfg       Config
		wantError string
	}{
		{"disabled", Config{}, ""},
		{"reject_unprotected", Config{CloudTenantAuthzEnabled: true}, "CLOUD_TENANT_AUTHZ_ENABLED requires"},
		{"reject_bearer", Config{CloudTenantAuthzEnabled: true, ControlPlaneAuthEnabled: true, ControlPlaneAuthMode: "bearer", ControlPlaneOIDCRBAC: true}, "CLOUD_TENANT_AUTHZ_ENABLED requires"},
		{"reject_hybrid", Config{CloudTenantAuthzEnabled: true, ControlPlaneAuthEnabled: true, ControlPlaneAuthMode: "hybrid", ControlPlaneOIDCRBAC: true}, "CLOUD_TENANT_AUTHZ_ENABLED requires"},
		{"reject_no_rbac", Config{CloudTenantAuthzEnabled: true, ControlPlaneAuthEnabled: true, ControlPlaneAuthMode: "oidc"}, "CLOUD_TENANT_AUTHZ_ENABLED requires"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantError == "" {
				// Other unrelated configuration checks may fail when called on
				// a deliberately empty Config; only check tenant authz error.
				if err != nil && strings.Contains(err.Error(), "CLOUD_TENANT_AUTHZ_ENABLED") {
					t.Fatalf("disabled mode should not trigger tenant authz validation: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tc.wantError)
			}
		})
	}
}
