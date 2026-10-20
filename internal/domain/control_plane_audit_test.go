package domain

import (
	"testing"
	"time"
)

func TestComputeControlPlaneAuditHashDeterministicAndSensitive(t *testing.T) {
	created := time.Date(2026, 9, 8, 3, 0, 0, 123456789, time.UTC)
	completed := time.Date(2026, 9, 8, 3, 0, 1, 987654321, time.UTC)
	sequence := uint64(7)
	audit := ControlPlaneAudit{
		ID:             42,
		OperatorID:     "platform-admin",
		CredentialSlot: "active",
		Method:         "PUT",
		Route:          "/api/v1/ai/routing-policy",
		Target:         "/api/v1/ai/routing-policy",
		RequestID:      "request-1",
		StatusCode:     200,
		Outcome:        "success",
		ChainSequence:  &sequence,
		PreviousHash:   "abc123",
		CreatedAt:      created,
		CompletedAt:    &completed,
	}

	first, err := ComputeControlPlaneAuditHash(audit)
	if err != nil {
		t.Fatalf("ComputeControlPlaneAuditHash() error = %v", err)
	}
	second, err := ComputeControlPlaneAuditHash(audit)
	if err != nil {
		t.Fatalf("ComputeControlPlaneAuditHash() second error = %v", err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("hashes = %q and %q, want same 64-char SHA-256", first, second)
	}

	tampered := audit
	tampered.OperatorID = "attacker"
	tamperedHash, err := ComputeControlPlaneAuditHash(tampered)
	if err != nil {
		t.Fatalf("tampered hash error = %v", err)
	}
	if tamperedHash == first {
		t.Fatal("hash did not change after audit content changed")
	}

	// Sub-millisecond database precision differences must not change the
	// canonical record hash.
	precisionVariant := audit
	precisionVariant.CreatedAt = created.Truncate(time.Millisecond).Add(999 * time.Microsecond)
	precisionVariant.CompletedAt = ptrTime(completed.Truncate(time.Millisecond).Add(999 * time.Microsecond))
	variantHash, err := ComputeControlPlaneAuditHash(precisionVariant)
	if err != nil {
		t.Fatalf("precision variant hash error = %v", err)
	}
	if variantHash != first {
		t.Fatalf("canonical millisecond hash changed: %q != %q", variantHash, first)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
