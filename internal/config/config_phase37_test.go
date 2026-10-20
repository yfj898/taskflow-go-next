package config

import "testing"

func TestValidateSIEMPromotionRolePolicyPhase37Reviewer(t *testing.T) {
	allowed := map[string]struct{}{
		"requester": {}, "approver": {}, "commander": {}, "reviewer": {},
	}
	policy := "requester=requester;approver=approver;commander=incident-commander;reviewer=governance-reviewer"
	if err := validateSIEMPromotionRolePolicy(policy, allowed, 1, true, true); err != nil {
		t.Fatal(err)
	}
	withoutReviewer := "requester=requester;approver=approver;commander=incident-commander"
	if err := validateSIEMPromotionRolePolicy(withoutReviewer, allowed, 1, true, true); err == nil {
		t.Fatal("expected governance-reviewer requirement")
	}
}
