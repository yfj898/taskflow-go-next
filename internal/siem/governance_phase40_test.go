package siem

import (
	"testing"
	"time"
)

func TestGovernancePreviousReceiptKeyGraceIsBounded(t *testing.T) {
	now := time.Now().UTC()
	replica := governanceReplica{
		id:                           "governance-primary",
		receiptKeyID:                 "primary-v2",
		receiptHMACKey:               []byte("new-secret"),
		receiptKeyGeneration:         2,
		previousReceiptKeyID:         "primary-v1",
		previousReceiptHMACKey:       []byte("old-secret"),
		previousReceiptKeyGeneration: 1,
		previousReceiptValidUntil:    now.Add(time.Minute),
	}
	verification, err := governanceReceiptVerificationKey(replica, "primary-v1", now)
	if err != nil {
		t.Fatal(err)
	}
	if verification.state != "previous" || verification.generation != 1 {
		t.Fatalf("unexpected previous-key verification: %+v", verification)
	}
	if _, err := governanceReceiptVerificationKey(replica, "primary-v1", now.Add(2*time.Minute)); err == nil {
		t.Fatal("expected expired previous receipt key to be rejected")
	}
}

func TestGovernanceHAClientRequiresMonotonicRotationMetadata(t *testing.T) {
	signer, err := NewHMACSHA256Signer("outbound-v1", "outbound-secret")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour)
	_, err = NewGovernanceHAClient([]GovernanceReplicaConfig{
		{
			ID: "primary", Endpoint: "http://primary.example", ReceiptKeyID: "primary-v2", ReceiptHMACKey: "primary-new", ReceiptKeyGeneration: 2,
			PreviousReceiptKeyID: "primary-v1", PreviousReceiptHMACKey: "primary-old", PreviousReceiptKeyGeneration: 2, PreviousReceiptValidUntil: deadline,
		},
		{ID: "secondary", Endpoint: "http://secondary.example", ReceiptKeyID: "secondary-v2", ReceiptHMACKey: "secondary-new", ReceiptKeyGeneration: 2},
	}, 2, 1, signer, time.Second)
	if err == nil {
		t.Fatal("expected previous generation equal to active generation to be rejected")
	}

	_, err = NewGovernanceHAClient([]GovernanceReplicaConfig{
		{
			ID: "primary", Endpoint: "http://primary.example", ReceiptKeyID: "primary-v2", ReceiptHMACKey: "primary-new", ReceiptKeyGeneration: 2,
			PreviousReceiptKeyID: "primary-v1", PreviousReceiptHMACKey: "primary-old", PreviousReceiptKeyGeneration: 1,
		},
		{ID: "secondary", Endpoint: "http://secondary.example", ReceiptKeyID: "secondary-v2", ReceiptHMACKey: "secondary-new", ReceiptKeyGeneration: 2},
	}, 2, 1, signer, time.Second)
	if err == nil {
		t.Fatal("expected previous receipt key without bounded validity deadline to be rejected")
	}
}

func TestGovernancePreviousKeyGraceCount(t *testing.T) {
	now := time.Now().UTC()
	client := &GovernanceClient{replicas: []governanceReplica{
		{id: "primary", previousReceiptKeyID: "p1", previousReceiptValidUntil: now.Add(time.Minute)},
		{id: "secondary", previousReceiptKeyID: "s1", previousReceiptValidUntil: now.Add(-time.Minute)},
	}, quorum: 2}
	if got := client.PreviousKeyGraceCount(now); got != 1 {
		t.Fatalf("expected one previous-key grace replica, got %d", got)
	}
}
