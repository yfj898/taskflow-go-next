package config

import "testing"

func TestPhase41OIDCValidation(t *testing.T) {
	t.Setenv("CONTROL_PLANE_AUTH_ENABLED", "true")
	t.Setenv("CONTROL_PLANE_AUTH_MODE", "oidc")
	t.Setenv("CONTROL_PLANE_OIDC_ISSUER", "https://issuer.example")
	t.Setenv("CONTROL_PLANE_OIDC_AUDIENCE", "taskflow-control-plane")
	t.Setenv("CONTROL_PLANE_OIDC_JWKS_URL", "https://issuer.example/jwks")
	t.Setenv("CONTROL_PLANE_OIDC_ROLE_MAPPINGS", "ops=control-plane-operator")
	t.Setenv("CONTROL_PLANE_OIDC_RBAC_ENABLED", "true")
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid OIDC config rejected: %v", err)
	}
	t.Setenv("CONTROL_PLANE_OIDC_JWKS_URL", "")
	cfg = Load()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing JWKS URL rejection")
	}
}
