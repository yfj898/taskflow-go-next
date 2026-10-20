package siem

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func newPhase38GovernanceServer(t *testing.T, replicaID, receiptKeyID, receiptKey, faultRecordHash string) *httptest.Server {
	t.Helper()
	receiptSigner, err := NewHMACSHA256Signer(receiptKeyID, receiptKey)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/anchors", func(w http.ResponseWriter, r *http.Request) {
		var envelope governanceSignedEnvelope
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		var payload termAnchorPayload
		if err := json.Unmarshal([]byte(envelope.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		recordHash := payload.RecordHash
		if faultRecordHash != "" {
			recordHash = faultRecordHash
		}
		anchorID := "phase38-anchor"
		anchoredAt := "2026-09-09T05:00:00Z"
		identity, _ := json.Marshal(map[string]any{"anchor_id": anchorID, "record_hash": recordHash, "term": payload.Term})
		receiptHash := governanceReceiptHash(identity)
		signedPayload, _ := json.Marshal(map[string]any{
			"anchor_id": anchorID, "anchored_at": anchoredAt, "record_hash": recordHash,
			"receipt_hash": receiptHash, "receipt_key_generation": uint64(1), "replica_id": replicaID, "term": payload.Term, "membership_epoch": uint64(1),
		})
		receiptSignature, _ := receiptSigner.Sign(signedPayload)
		_ = json.NewEncoder(w).Encode(governanceAnchorReceipt{
			AnchorID: anchorID, Term: payload.Term, RecordHash: recordHash, ReceiptHash: receiptHash, AnchoredAt: anchoredAt,
			ReplicaID: replicaID, ReceiptKeyID: receiptKeyID, ReceiptKeyGeneration: 1, ReceiptAlgorithm: "hmac-sha256", ReceiptSignature: receiptSignature, MembershipEpoch: 1,
		})
	})
	mux.HandleFunc("/alerts", func(w http.ResponseWriter, r *http.Request) {
		var envelope governanceSignedEnvelope
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(envelope.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		evidenceHash, _ := payload["evidence_hash"].(string)
		identity, _ := json.Marshal(map[string]any{"evidence_hash": evidenceHash, "status": "accepted"})
		receiptHash := governanceReceiptHash(identity)
		signedPayload, _ := json.Marshal(map[string]any{
			"evidence_hash": evidenceHash, "receipt_hash": receiptHash, "receipt_key_generation": uint64(1), "replica_id": replicaID, "status": "accepted", "membership_epoch": uint64(1),
		})
		receiptSignature, _ := receiptSigner.Sign(signedPayload)
		_ = json.NewEncoder(w).Encode(governanceAlertReceipt{
			Status: "accepted", EvidenceHash: evidenceHash, ReceiptHash: receiptHash,
			ReplicaID: replicaID, ReceiptKeyID: receiptKeyID, ReceiptKeyGeneration: 1, ReceiptAlgorithm: "hmac-sha256", ReceiptSignature: receiptSignature, MembershipEpoch: 1,
		})
	})
	return httptest.NewServer(mux)
}

func TestGovernanceHAClientRequiresTwoVerifiedReplicaReceipts(t *testing.T) {
	primary := newPhase38GovernanceServer(t, "primary", "primary-receipt-v1", "primary-secret", "")
	defer primary.Close()
	secondary := newPhase38GovernanceServer(t, "secondary", "secondary-receipt-v1", "secondary-secret", "")
	defer secondary.Close()
	outboundSigner, err := NewHMACSHA256Signer("outbound-v1", "outbound-secret")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGovernanceHAClient([]GovernanceReplicaConfig{
		{ID: "primary", Endpoint: primary.URL, ReceiptKeyID: "primary-receipt-v1", ReceiptHMACKey: "primary-secret", ReceiptKeyGeneration: 1},
		{ID: "secondary", Endpoint: secondary.URL, ReceiptKeyID: "secondary-receipt-v1", ReceiptHMACKey: "secondary-secret", ReceiptKeyGeneration: 1},
	}, 2, 1, outboundSigner, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entry := domain.ControlPlaneSIEMReceiverTermHistory{
		ID: 17, Term: 3, ActiveReceiver: "receiver-primary",
		RecordHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	anchor, receipts, err := client.AnchorTermWithReplicaReceipts(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if anchor.Term != entry.Term || len(receipts) != 2 || receipts[0].ReplicaID == receipts[1].ReplicaID {
		t.Fatalf("unexpected HA anchor result anchor=%+v receipts=%+v", anchor, receipts)
	}
	evidence := domain.ControlPlaneSIEMIncidentEvidence{
		ID: 23, IncidentID: "INC-38", Kind: "quorum", Severity: "critical",
		EvidenceHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	alertReceipts, err := client.AlertIncidentWithReplicaReceipts(context.Background(), evidence)
	if err != nil {
		t.Fatal(err)
	}
	if len(alertReceipts) != 2 {
		t.Fatalf("expected two alert receipts, got %+v", alertReceipts)
	}
}

func TestGovernanceHAClientRejectsSignedReplicaIdentityConflict(t *testing.T) {
	primary := newPhase38GovernanceServer(t, "primary", "primary-receipt-v1", "primary-secret", "")
	defer primary.Close()
	secondary := newPhase38GovernanceServer(t, "secondary", "secondary-receipt-v1", "secondary-secret", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	defer secondary.Close()
	outboundSigner, err := NewHMACSHA256Signer("outbound-v1", "outbound-secret")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGovernanceHAClient([]GovernanceReplicaConfig{
		{ID: "primary", Endpoint: primary.URL, ReceiptKeyID: "primary-receipt-v1", ReceiptHMACKey: "primary-secret", ReceiptKeyGeneration: 1},
		{ID: "secondary", Endpoint: secondary.URL, ReceiptKeyID: "secondary-receipt-v1", ReceiptHMACKey: "secondary-secret", ReceiptKeyGeneration: 1},
	}, 2, 1, outboundSigner, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, receipts, err := client.AnchorTermWithReplicaReceipts(context.Background(), domain.ControlPlaneSIEMReceiverTermHistory{
		ID: 9, Term: 4, RecordHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	})
	if err == nil || !IsGovernanceConsistencyError(err) {
		t.Fatalf("expected signed consistency failure, receipts=%+v err=%v", receipts, err)
	}
}
