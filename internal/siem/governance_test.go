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

func TestGovernanceClientAnchorAndAlertReceipts(t *testing.T) {
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
		identity, _ := json.Marshal(map[string]any{
			"anchor_id":   "anchor-test",
			"record_hash": payload.RecordHash,
			"term":        payload.Term,
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"anchor_id":    "anchor-test",
			"term":         payload.Term,
			"record_hash":  payload.RecordHash,
			"receipt_hash": governanceReceiptHash(identity),
			"anchored_at":  "2026-09-08T13:00:00Z",
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
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":        "accepted",
			"evidence_hash": payload["evidence_hash"],
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	signer, err := NewHMACSHA256Signer("governance-test", "secret")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGovernanceClient(server.URL, "", signer, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entry := domain.ControlPlaneSIEMReceiverTermHistory{
		Term:           2,
		ActiveReceiver: "secondary",
		RecordHash:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	anchor, err := client.AnchorTerm(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	if anchor.Term != 2 || anchor.AnchorID != "anchor-test" || anchor.ReceiptHash == "" {
		t.Fatalf("unexpected anchor: %+v", anchor)
	}
	evidence := domain.ControlPlaneSIEMIncidentEvidence{
		IncidentID:   "incident-test",
		Kind:         "rpo_breach",
		Severity:     "critical",
		EvidenceHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	if err := client.AlertIncident(context.Background(), evidence); err != nil {
		t.Fatal(err)
	}
}

func TestOperatorHasRole(t *testing.T) {
	policy := "requester=requester;approver-a=approver;commander=incident-commander|approver"
	if !operatorHasRole(policy, "requester", "requester") {
		t.Fatal("requester role not found")
	}
	if !operatorHasRole(policy, "commander", "incident-commander") {
		t.Fatal("incident commander role not found")
	}
	if operatorHasRole(policy, "requester", "approver") {
		t.Fatal("requester unexpectedly has approver role")
	}
}
