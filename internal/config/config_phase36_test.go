package config

import "testing"

func TestValidateSIEMPromotionRolePolicyRequiresGovernanceReviewer(t *testing.T) {
	allowed := map[string]struct{}{
		"requester": {},
		"approver":  {},
		"commander": {},
		"reviewer":  {},
	}
	policy := "requester=requester;approver=approver;commander=incident-commander;reviewer=governance-reviewer"
	if err := validateSIEMPromotionRolePolicy(policy, allowed, 1, true, true); err != nil {
		t.Fatal(err)
	}
	missingReviewer := "requester=requester;approver=approver;commander=incident-commander"
	if err := validateSIEMPromotionRolePolicy(missingReviewer, allowed, 1, true, true); err == nil {
		t.Fatal("expected missing governance-reviewer rejection")
	}
}
