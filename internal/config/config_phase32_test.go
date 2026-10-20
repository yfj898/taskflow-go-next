package config

import "testing"

func TestParseAdditionalControlPlaneCredentials(t *testing.T) {
	credentials, err := parseAdditionalControlPlaneCredentials("approver-a=token-a;commander=token-b")
	if err != nil {
		t.Fatal(err)
	}
	if credentials["approver-a"] != "token-a" || credentials["commander"] != "token-b" {
		t.Fatalf("unexpected credentials metadata: %#v", credentials)
	}
	if _, err := parseAdditionalControlPlaneCredentials("a=token;a=other"); err == nil {
		t.Fatal("expected duplicate operator rejection")
	}
	if _, err := parseAdditionalControlPlaneCredentials("a=token;b=token"); err == nil {
		t.Fatal("expected duplicate token rejection")
	}
}

func TestValidateSIEMPromotionRolePolicy(t *testing.T) {
	allowed := map[string]struct{}{
		"requester":  {},
		"approver-a": {},
		"approver-b": {},
		"commander":  {},
	}
	policy := "requester=requester;approver-a=approver;approver-b=approver;commander=incident-commander"
	if err := validateSIEMPromotionRolePolicy(policy, allowed, 2, true, false); err != nil {
		t.Fatal(err)
	}
	if err := validateSIEMPromotionRolePolicy(policy, allowed, 3, true, false); err == nil {
		t.Fatal("expected insufficient approver rejection")
	}
	if err := validateSIEMPromotionRolePolicy("requester=requester;unknown=approver", allowed, 1, false, false); err == nil {
		t.Fatal("expected unauthenticated operator rejection")
	}
}
