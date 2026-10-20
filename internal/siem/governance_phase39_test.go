package siem

import (
	"context"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func TestGovernanceHAReceiptPreviousKeySupportsRollingRotation(t *testing.T) {
	primary := newPhase38GovernanceServer(t, "primary", "primary-old", "primary-old-secret", "")
	defer primary.Close()
	secondary := newPhase38GovernanceServer(t, "secondary", "secondary-old", "secondary-old-secret", "")
	defer secondary.Close()
	outboundSigner, err := NewHMACSHA256Signer("outbound-v1", "outbound-secret")
	if err != nil {
		t.Fatal(err)
	}
	previousValidUntil := time.Now().UTC().Add(time.Hour)
	client, err := NewGovernanceHAClient([]GovernanceReplicaConfig{
		{
			ID: "primary", Endpoint: primary.URL,
			ReceiptKeyID: "primary-new", ReceiptHMACKey: "primary-new-secret", ReceiptKeyGeneration: 2,
			PreviousReceiptKeyID: "primary-old", PreviousReceiptHMACKey: "primary-old-secret", PreviousReceiptKeyGeneration: 1, PreviousReceiptValidUntil: previousValidUntil,
		},
		{
			ID: "secondary", Endpoint: secondary.URL,
			ReceiptKeyID: "secondary-new", ReceiptHMACKey: "secondary-new-secret", ReceiptKeyGeneration: 2,
			PreviousReceiptKeyID: "secondary-old", PreviousReceiptHMACKey: "secondary-old-secret", PreviousReceiptKeyGeneration: 1, PreviousReceiptValidUntil: previousValidUntil,
		},
	}, 2, 1, outboundSigner, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, receipts, err := client.AnchorTermWithReplicaReceipts(context.Background(), domain.ControlPlaneSIEMReceiverTermHistory{
		ID: 39, Term: 9, ActiveReceiver: "receiver-primary",
		RecordHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 2 || receipts[0].MembershipEpoch != 1 || receipts[1].MembershipEpoch != 1 {
		t.Fatalf("unexpected rolling-rotation receipts: %+v", receipts)
	}
}

func TestGovernanceHARejectsStaleMembershipEpochDespiteValidSignature(t *testing.T) {
	primary := newPhase38GovernanceServer(t, "primary", "primary-v2", "primary-secret", "")
	defer primary.Close()
	secondary := newPhase38GovernanceServer(t, "secondary", "secondary-v2", "secondary-secret", "")
	defer secondary.Close()
	outboundSigner, err := NewHMACSHA256Signer("outbound-v1", "outbound-secret")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGovernanceHAClient([]GovernanceReplicaConfig{
		{ID: "primary", Endpoint: primary.URL, ReceiptKeyID: "primary-v2", ReceiptHMACKey: "primary-secret", ReceiptKeyGeneration: 1},
		{ID: "secondary", Endpoint: secondary.URL, ReceiptKeyID: "secondary-v2", ReceiptHMACKey: "secondary-secret", ReceiptKeyGeneration: 1},
	}, 2, 2, outboundSigner, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.AnchorTermWithReplicaReceipts(context.Background(), domain.ControlPlaneSIEMReceiverTermHistory{
		ID: 40, Term: 10, ActiveReceiver: "receiver-secondary",
		RecordHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
	if err == nil || !IsGovernanceConsistencyError(err) {
		t.Fatalf("expected stale membership epoch consistency failure, got %v", err)
	}
}
