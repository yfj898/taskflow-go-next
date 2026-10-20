package domain

import "testing"

func TestVerificationVerdict(t *testing.T) {
	for _, tc := range []struct {
		name   string
		checks []VerificationCheck
		want   string
	}{
		{"all required pass", []VerificationCheck{{Required: true, Status: "passed"}}, "passed"},
		{"failed dominates not run", []VerificationCheck{{Required: true, Status: "not_run"}, {Required: true, Status: "failed"}}, "failed"},
		{"incomplete", []VerificationCheck{{Required: true, Status: "not_run"}}, "incomplete"},
		{"optional failure visible", []VerificationCheck{{Required: true, Status: "passed"}, {Required: false, Status: "failed"}}, "passed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := VerificationVerdict(tc.checks); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
