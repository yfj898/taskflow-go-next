package siem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	archivepkg "github.com/yfj898/taskflow-go/internal/archive"
)

func TestMultiSinkEvidenceReplicaWritesAndVerifiesBothCopies(t *testing.T) {
	primary, err := archivepkg.NewFileSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := archivepkg.NewFileSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replicator, err := NewMultiSinkEvidenceReplica(2, primary, replica)
	if err != nil {
		t.Fatal(err)
	}
	key := "control-plane-audit/0001-test.json"
	payload := []byte(`{"version":"evidence-v1"}`)
	copies, err := replicator.Replicate(context.Background(), key, payload)
	if err != nil {
		t.Fatal(err)
	}
	if copies != 2 {
		t.Fatalf("copies = %d, want 2", copies)
	}
	for _, sink := range []archivepkg.Sink{primary, replica} {
		got, err := sink.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("replicated payload = %q, want %q", got, payload)
		}
	}
	if copies, err := replicator.Replicate(context.Background(), key, payload); err != nil || copies != 2 {
		t.Fatalf("idempotent replicate copies=%d err=%v", copies, err)
	}
}

func TestReceiverReceiptSupplementExtendsImmutableEvidence(t *testing.T) {
	primary, err := archivepkg.NewFileSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := archivepkg.NewFileSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	replicator, err := NewMultiSinkEvidenceReplica(2, primary, replica)
	if err != nil {
		t.Fatal(err)
	}
	secondaryProof := deliveryReceiptProof{
		IdempotencyKey:     "taskflow-audit-test",
		FirstSequence:      7,
		LastSequence:       7,
		PayloadSHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ReceiptID:          "receipt-secondary",
		ReceiverID:         "receiver-secondary",
		ReceiverGeneration: 1,
		ReceiverTerm:       2,
	}
	proofBody, _ := json.Marshal(secondaryProof)
	proofDigest := sha256.Sum256(proofBody)
	proofSetBody, _ := json.Marshal([]deliveryReceiptProof{secondaryProof})
	proofSetDigest := sha256.Sum256(proofSetBody)
	evidence, _ := json.Marshal(deliveryEvidence{
		Version:                "taskflow-control-plane-siem-evidence-v1",
		IdempotencyKey:         secondaryProof.IdempotencyKey,
		FirstSequence:          7,
		LastSequence:           7,
		PayloadSHA256:          secondaryProof.PayloadSHA256,
		ReceiverReceipt:        secondaryProof,
		ReceiptProofSHA256:     hex.EncodeToString(proofDigest[:]),
		ReceiverReceipts:       []deliveryReceiptProof{secondaryProof},
		ReceiverReceiptsSHA256: hex.EncodeToString(proofSetDigest[:]),
	})
	key := deliveryEvidenceObjectKey(7, 7, secondaryProof.IdempotencyKey)
	if copies, err := replicator.Replicate(context.Background(), key, evidence); err != nil || copies != 2 {
		t.Fatalf("replicate base evidence copies=%d err=%v", copies, err)
	}
	primaryProof := secondaryProof
	primaryProof.ReceiptID = "receipt-primary"
	primaryProof.ReceiverID = "receiver-primary"
	if _, _, err := replicator.VerifyReceiptEvidence(context.Background(), key, primaryProof, 2); err == nil {
		t.Fatal("primary proof unexpectedly verified before anti-entropy supplement")
	}
	if copies, err := replicator.ReplicateReceiverReceipt(context.Background(), 7, primaryProof.IdempotencyKey, primaryProof); err != nil || copies != 2 {
		t.Fatalf("replicate receiver supplement copies=%d err=%v", copies, err)
	}
	if copies, _, err := replicator.VerifyReceiptEvidence(context.Background(), key, primaryProof, 2); err != nil || copies != 2 {
		t.Fatalf("verify supplemented receiver proof copies=%d err=%v", copies, err)
	}
}
