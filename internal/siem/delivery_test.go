package siem

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type fakeDeliveryRepository struct {
	deliveries []domain.ControlPlaneAuditDelivery
	checkpoint domain.ControlPlaneAuditChainState
	delivered  []uint64
	ack        domain.ControlPlaneSIEMAcknowledgement
	failed     bool
}

func (f *fakeDeliveryRepository) ActivateControlPlaneSIEMDeliveryFence(_ context.Context, candidate uint64) (uint64, error) {
	if candidate == 0 {
		return 1, nil
	}
	return candidate, nil
}

func (f *fakeDeliveryRepository) ClaimControlPlaneAuditDeliveries(context.Context, time.Time, time.Time, string, uint64, int) ([]domain.ControlPlaneAuditDelivery, error) {
	claimed := append([]domain.ControlPlaneAuditDelivery(nil), f.deliveries...)
	for i := range claimed {
		claimed[i].Attempts++
	}
	f.deliveries = nil
	return claimed, nil
}

func (f *fakeDeliveryRepository) MarkControlPlaneAuditDeliveriesDelivered(_ context.Context, ids []uint64, _ string, _ uint64, _ time.Time, ack domain.ControlPlaneSIEMAcknowledgement) error {
	f.delivered = append([]uint64(nil), ids...)
	f.ack = ack
	return nil
}

func (f *fakeDeliveryRepository) FailControlPlaneAuditDeliveries(context.Context, []domain.ControlPlaneAuditDelivery, string, uint64, time.Time, time.Time, error, int) (int, int, error) {
	f.failed = true
	return 1, 0, nil
}

func (f *fakeDeliveryRepository) GetControlPlaneAuditCheckpoint(context.Context) (domain.ControlPlaneAuditChainState, error) {
	return f.checkpoint, nil
}

func (f *fakeDeliveryRepository) ControlPlaneAuditDeliveryStats(context.Context) (domain.ControlPlaneAuditDeliveryStats, error) {
	return domain.ControlPlaneAuditDeliveryStats{}, nil
}

func TestDeliveryWorkerPostsSignedEnvelope(t *testing.T) {
	const signingKey = "phase25-signing-key"
	const previousSigningKey = "phase26-previous-signing-key"
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		received = body
		mac := hmac.New(sha256.New, []byte(signingKey))
		_, _ = mac.Write(body)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := r.Header.Get("X-TaskFlow-Audit-Signature"); got != want {
			t.Fatalf("signature = %q, want %q", got, want)
		}
		if got := r.Header.Get("X-TaskFlow-Audit-Key-ID"); got != "key-v1" {
			t.Fatalf("key id = %q", got)
		}
		if got := r.Header.Get("X-TaskFlow-Audit-Signature-Algorithm"); got != "hmac-sha256" {
			t.Fatalf("signature algorithm = %q", got)
		}
		previousMAC := hmac.New(sha256.New, []byte(previousSigningKey))
		_, _ = previousMAC.Write(body)
		previousWant := "sha256=" + hex.EncodeToString(previousMAC.Sum(nil))
		if got := r.Header.Get("X-TaskFlow-Audit-Previous-Signature"); got != previousWant {
			t.Fatalf("previous signature = %q, want %q", got, previousWant)
		}
		if got := r.Header.Get("X-TaskFlow-Audit-Previous-Key-ID"); got != "key-v0" {
			t.Fatalf("previous key id = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sink-token" {
			t.Fatalf("Authorization = %q", got)
		}
		if r.Header.Get("Idempotency-Key") == "" {
			t.Fatal("missing Idempotency-Key")
		}
		payloadDigest := sha256.Sum256(body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(deliveryAcknowledgement{
			Status:         "accepted",
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
			FirstSequence:  12,
			LastSequence:   12,
			PayloadSHA256:  hex.EncodeToString(payloadDigest[:]),
			ReceiptID:      "receipt-test-12",
		})
	}))
	defer server.Close()

	repo := &fakeDeliveryRepository{
		deliveries: []domain.ControlPlaneAuditDelivery{{
			ID:            7,
			AuditID:       11,
			ChainSequence: 12,
			RecordHash:    "abc123",
			PayloadJSON:   `{"id":11,"chain_sequence":12,"record_hash":"abc123"}`,
		}},
		checkpoint: domain.ControlPlaneAuditChainState{LastSequence: 12, HeadHash: "abc123"},
	}
	signer, err := NewRotatingHMACSHA256Signer("key-v1", signingKey, "key-v0", previousSigningKey)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(repo, DeliveryConfig{
		Endpoint:    server.URL,
		BearerToken: "sink-token",
		HTTPTimeout: time.Second,
	}, signer)
	if err != nil {
		t.Fatal(err)
	}
	worker.runOnce(context.Background(), 0)
	if len(received) == 0 {
		t.Fatal("SIEM server received no body")
	}
	if len(repo.delivered) != 1 || repo.delivered[0] != 7 {
		t.Fatalf("delivered ids = %v, want [7]", repo.delivered)
	}
	if repo.ack.ReceiptID != "receipt-test-12" || repo.ack.PayloadSHA256 == "" || repo.ack.AckHash == "" {
		t.Fatalf("unexpected acknowledgement: %+v", repo.ack)
	}
	if repo.failed {
		t.Fatal("failure settlement unexpectedly called")
	}
}

func TestRetryDelayCapsExponentially(t *testing.T) {
	base := time.Second
	max := 8 * time.Second
	for attempts, want := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		8: 8 * time.Second,
	} {
		if got := retryDelay(base, max, attempts); got != want {
			t.Fatalf("retryDelay attempts=%d = %v, want %v", attempts, got, want)
		}
	}
}

func TestDeliveryEnvelopeAndIdempotencySurviveOutboxRebuild(t *testing.T) {
	completed := time.Date(2026, 9, 8, 4, 30, 0, 0, time.UTC)
	sequence := uint64(77)
	audit := domain.ControlPlaneAudit{
		ID:            51,
		ChainSequence: &sequence,
		RecordHash:    "record-hash-77",
		CompletedAt:   &completed,
		CreatedAt:     completed.Add(-time.Second),
	}
	payload, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	one := domain.ControlPlaneAuditDelivery{
		ID:            7,
		ChainSequence: 77,
		RecordHash:    "record-hash-77",
		PayloadJSON:   string(payload),
		CreatedAt:     completed.Add(time.Minute),
	}
	two := one
	two.ID = 999
	two.CreatedAt = completed.Add(24 * time.Hour)
	repo := &fakeDeliveryRepository{}
	signer, err := NewHMACSHA256Signer("key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(repo, DeliveryConfig{Endpoint: "http://example.test"}, signer)
	if err != nil {
		t.Fatal(err)
	}
	bodyOne, _, _, err := worker.buildEnvelope(context.Background(), []domain.ControlPlaneAuditDelivery{one}, completed)
	if err != nil {
		t.Fatal(err)
	}
	bodyTwo, _, _, err := worker.buildEnvelope(context.Background(), []domain.ControlPlaneAuditDelivery{two}, completed.Add(48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyOne, bodyTwo) {
		t.Fatalf("recreated outbox changed SIEM payload:\n%s\n%s", bodyOne, bodyTwo)
	}
	if deliveryIdempotencyKey([]domain.ControlPlaneAuditDelivery{one}) != deliveryIdempotencyKey([]domain.ControlPlaneAuditDelivery{two}) {
		t.Fatal("recreated outbox changed idempotency key")
	}
}
