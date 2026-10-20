package siem

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type checkpointRepoFake struct {
	audit    domain.ControlPlaneAudit
	receipts []domain.ControlPlaneAuditDeliveryReceipt
}

func (f *checkpointRepoFake) GetControlPlaneAuditBySequence(_ context.Context, sequence uint64) (domain.ControlPlaneAudit, error) {
	if f.audit.ChainSequence == nil || *f.audit.ChainSequence != sequence {
		return domain.ControlPlaneAudit{}, fmt.Errorf("not found")
	}
	return f.audit, nil
}

func (f *checkpointRepoFake) ImportControlPlaneAuditDeliveryReceipt(_ context.Context, receipt domain.ControlPlaneAuditDeliveryReceipt) (bool, error) {
	f.receipts = append(f.receipts, receipt)
	return true, nil
}

type checkpointEvidenceFake struct {
	key   string
	proof deliveryReceiptProof
}

func (f *checkpointEvidenceFake) VerifyReceiptEvidence(_ context.Context, key string, proof deliveryReceiptProof, minCopies int) (int, string, error) {
	f.key = key
	f.proof = proof
	if minCopies != 1 {
		return 0, "", fmt.Errorf("min copies = %d", minCopies)
	}
	return 1, "siem-dr-replica", nil
}

func TestCheckpointReconcilerVerifiesCanonicalAuditAndEvidence(t *testing.T) {
	sequence := uint64(1)
	createdAt := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	completedAt := createdAt.Add(time.Second)
	audit := domain.ControlPlaneAudit{
		ID:             10,
		OperatorID:     "phase28",
		CredentialSlot: "active",
		Method:         http.MethodGet,
		Route:          "/api/v1/ai/routing-policy",
		Target:         "routing-policy",
		RequestID:      "phase28-reconcile-test",
		StatusCode:     200,
		Outcome:        "success",
		ChainSequence:  &sequence,
		PreviousHash:   "",
		CreatedAt:      createdAt,
		CompletedAt:    &completedAt,
	}
	recordHash, err := domain.ComputeControlPlaneAuditHash(audit)
	if err != nil {
		t.Fatal(err)
	}
	audit.RecordHash = recordHash
	_, idempotencyKey, payloadSHA, err := canonicalDeliveryIdentityFromAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	receiptID := "receiver-receipt-1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer checkpoint-token" {
			t.Fatal("missing checkpoint bearer token")
		}
		_ = json.NewEncoder(w).Encode(receiverCheckpointResponse{
			Version:               "taskflow-control-plane-siem-checkpoint-v1",
			HighWatermarkSequence: sequence,
			HeadRecordHash:        recordHash,
			Items: []receiverCheckpointReceipt{{
				ChainSequence:  sequence,
				RecordHash:     recordHash,
				IdempotencyKey: idempotencyKey,
				PayloadSHA256:  payloadSHA,
				ReceiptID:      receiptID,
				AcceptedAt:     completedAt.Format(time.RFC3339Nano),
			}},
		})
	}))
	defer server.Close()

	repo := &checkpointRepoFake{audit: audit}
	evidence := &checkpointEvidenceFake{}
	reconciler, err := NewCheckpointReconciler(repo, CheckpointReconcilerConfig{
		Endpoint:          server.URL,
		BearerToken:       "checkpoint-token",
		HTTPTimeout:       time.Second,
		EvidenceVerifier:  evidence,
		MinEvidenceCopies: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	imported, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if imported != 1 || len(repo.receipts) != 1 {
		t.Fatalf("imported=%d receipts=%d", imported, len(repo.receipts))
	}
	got := repo.receipts[0]
	if got.AckStatus != "reconciled" || got.RecoverySource != "receiver-checkpoint" || got.ReplicaCopies != 1 {
		t.Fatalf("receipt = %+v", got)
	}
	if evidence.proof.ReceiptID != receiptID || evidence.proof.PayloadSHA256 != payloadSHA {
		t.Fatalf("evidence proof = %+v", evidence.proof)
	}
	if evidence.key == "" {
		t.Fatal("evidence key was not checked")
	}
}
