package repository

import (
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func TestVerificationIdempotencyFields(t *testing.T) {
	in := domain.VerificationRequest{ProjectID: "p", TargetCommit: "c", CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"b", "a"}, ExternalSnapshotID: "s"}
	run := &domain.VerificationRun{ProjectID: "p", TargetCommit: "c", CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDsJSON: `["a","b"]`, ExternalSnapshotID: "s"}
	if same, err := requestMatches(run, in); err != nil || !same {
		t.Fatal("unordered set not idempotent", same, err)
	}
	in.ExternalSnapshotID = "other"
	if same, err := requestMatches(run, in); err != nil || same {
		t.Fatal("snapshot mismatch accepted", same, err)
	}
	run.CheckIDsJSON = "not json"
	if _, err := requestMatches(run, in); err == nil {
		t.Fatal("corrupt persisted record hidden as conflict")
	}
}
