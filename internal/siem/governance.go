package siem

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type GovernanceClient struct {
	endpoint        string
	token           string
	signer          CheckpointSigner
	client          *http.Client
	replicas        []governanceReplica
	quorum          int
	membershipEpoch uint64
}

type GovernanceReplicaConfig struct {
	ID                           string
	Endpoint                     string
	Token                        string
	ReceiptKeyID                 string
	ReceiptHMACKey               string
	ReceiptKeyGeneration         uint64
	PreviousReceiptKeyID         string
	PreviousReceiptHMACKey       string
	PreviousReceiptKeyGeneration uint64
	PreviousReceiptValidUntil    time.Time
}

type governanceReplica struct {
	id                           string
	endpoint                     string
	token                        string
	receiptKeyID                 string
	receiptHMACKey               []byte
	receiptKeyGeneration         uint64
	previousReceiptKeyID         string
	previousReceiptHMACKey       []byte
	previousReceiptKeyGeneration uint64
	previousReceiptValidUntil    time.Time
}

type GovernanceConsistencyError struct {
	Kind      string
	ReplicaID string
	Reason    string
}

func (e *GovernanceConsistencyError) Error() string {
	return fmt.Sprintf("governance %s replica consistency failure replica=%s: %s", e.Kind, e.ReplicaID, e.Reason)
}

func IsGovernanceConsistencyError(err error) bool {
	var consistencyErr *GovernanceConsistencyError
	return errors.As(err, &consistencyErr)
}

func NewGovernanceClient(endpoint, token string, signer CheckpointSigner, timeout time.Duration) (*GovernanceClient, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("governance endpoint is empty")
	}
	if signer == nil {
		return nil, fmt.Errorf("governance signer is nil")
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &GovernanceClient{endpoint: endpoint, token: strings.TrimSpace(token), signer: signer, client: &http.Client{Timeout: timeout}, quorum: 1}, nil
}

func NewGovernanceHAClient(replicas []GovernanceReplicaConfig, quorum int, membershipEpoch uint64, signer CheckpointSigner, timeout time.Duration) (*GovernanceClient, error) {
	if signer == nil {
		return nil, fmt.Errorf("governance signer is nil")
	}
	if len(replicas) < 2 {
		return nil, fmt.Errorf("governance HA requires at least two replicas")
	}
	if quorum <= 1 || quorum > len(replicas) {
		return nil, fmt.Errorf("governance HA quorum must be between 2 and replica count")
	}
	if membershipEpoch == 0 {
		return nil, fmt.Errorf("governance HA membership epoch must be positive")
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	seenIDs := make(map[string]struct{}, len(replicas))
	seenEndpoints := make(map[string]struct{}, len(replicas))
	configured := make([]governanceReplica, 0, len(replicas))
	for _, replica := range replicas {
		id := strings.TrimSpace(replica.ID)
		endpoint := strings.TrimRight(strings.TrimSpace(replica.Endpoint), "/")
		keyID := strings.TrimSpace(replica.ReceiptKeyID)
		previousKeyID := strings.TrimSpace(replica.PreviousReceiptKeyID)
		if id == "" || endpoint == "" || keyID == "" || strings.TrimSpace(replica.ReceiptHMACKey) == "" || replica.ReceiptKeyGeneration == 0 {
			return nil, fmt.Errorf("governance HA replica requires id, endpoint, receipt key id and receipt HMAC key")
		}
		if (previousKeyID == "") != (strings.TrimSpace(replica.PreviousReceiptHMACKey) == "") {
			return nil, fmt.Errorf("governance HA previous receipt key id and key must be configured together replica=%q", id)
		}
		if previousKeyID != "" && previousKeyID == keyID {
			return nil, fmt.Errorf("governance HA previous receipt key id must differ from active key id replica=%q", id)
		}
		if previousKeyID != "" {
			if replica.PreviousReceiptKeyGeneration == 0 || replica.PreviousReceiptKeyGeneration >= replica.ReceiptKeyGeneration || replica.PreviousReceiptValidUntil.IsZero() {
				return nil, fmt.Errorf("governance HA previous receipt key requires older generation and explicit validity deadline replica=%q", id)
			}
		} else if replica.PreviousReceiptKeyGeneration != 0 || !replica.PreviousReceiptValidUntil.IsZero() {
			return nil, fmt.Errorf("governance HA previous receipt generation/deadline require previous key replica=%q", id)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("governance HA duplicate replica id %q", id)
		}
		if _, exists := seenEndpoints[endpoint]; exists {
			return nil, fmt.Errorf("governance HA duplicate replica endpoint %q", endpoint)
		}
		seenIDs[id] = struct{}{}
		seenEndpoints[endpoint] = struct{}{}
		configured = append(configured, governanceReplica{
			id:                           id,
			endpoint:                     endpoint,
			token:                        strings.TrimSpace(replica.Token),
			receiptKeyID:                 keyID,
			receiptHMACKey:               []byte(replica.ReceiptHMACKey),
			receiptKeyGeneration:         replica.ReceiptKeyGeneration,
			previousReceiptKeyID:         previousKeyID,
			previousReceiptHMACKey:       []byte(replica.PreviousReceiptHMACKey),
			previousReceiptKeyGeneration: replica.PreviousReceiptKeyGeneration,
			previousReceiptValidUntil:    replica.PreviousReceiptValidUntil.UTC(),
		})
	}
	sort.Slice(configured, func(i, j int) bool { return configured[i].id < configured[j].id })
	return &GovernanceClient{signer: signer, client: &http.Client{Timeout: timeout}, replicas: configured, quorum: quorum, membershipEpoch: membershipEpoch}, nil
}

func (g *GovernanceClient) HAEnabled() bool { return len(g.replicas) > 1 }

func (g *GovernanceClient) ReplicaCount() int {
	if g.HAEnabled() {
		return len(g.replicas)
	}
	if strings.TrimSpace(g.endpoint) != "" {
		return 1
	}
	return 0
}

func (g *GovernanceClient) Quorum() int {
	if g.quorum > 0 {
		return g.quorum
	}
	return 1
}

func (g *GovernanceClient) MembershipEpoch() uint64 { return g.membershipEpoch }

func (g *GovernanceClient) PreviousKeyGraceCount(now time.Time) int {
	if !g.HAEnabled() {
		return 0
	}
	now = now.UTC()
	count := 0
	for _, replica := range g.replicas {
		if replica.previousReceiptKeyID != "" && !replica.previousReceiptValidUntil.IsZero() && !now.After(replica.previousReceiptValidUntil) {
			count++
		}
	}
	return count
}

type termAnchorPayload struct {
	Version        string `json:"version"`
	Term           uint64 `json:"term"`
	RecordHash     string `json:"record_hash"`
	PreviousHash   string `json:"previous_hash"`
	ActiveReceiver string `json:"active_receiver"`
}

type governanceSignedEnvelope struct {
	Payload   string `json:"payload"`
	KeyID     string `json:"key_id"`
	Algorithm string `json:"algorithm"`
	Signature string `json:"signature"`
}

type governanceAnchorReceipt struct {
	AnchorID             string `json:"anchor_id"`
	Term                 uint64 `json:"term"`
	RecordHash           string `json:"record_hash"`
	ReceiptHash          string `json:"receipt_hash"`
	AnchoredAt           string `json:"anchored_at"`
	ReplicaID            string `json:"replica_id,omitempty"`
	ReceiptKeyID         string `json:"receipt_key_id,omitempty"`
	ReceiptKeyGeneration uint64 `json:"receipt_key_generation,omitempty"`
	ReceiptAlgorithm     string `json:"receipt_algorithm,omitempty"`
	ReceiptSignature     string `json:"receipt_signature,omitempty"`
	MembershipEpoch      uint64 `json:"membership_epoch,omitempty"`
}

type governanceAlertReceipt struct {
	Status               string `json:"status"`
	EvidenceHash         string `json:"evidence_hash"`
	ReceiptHash          string `json:"receipt_hash,omitempty"`
	ReplicaID            string `json:"replica_id,omitempty"`
	ReceiptKeyID         string `json:"receipt_key_id,omitempty"`
	ReceiptKeyGeneration uint64 `json:"receipt_key_generation,omitempty"`
	ReceiptAlgorithm     string `json:"receipt_algorithm,omitempty"`
	ReceiptSignature     string `json:"receipt_signature,omitempty"`
	MembershipEpoch      uint64 `json:"membership_epoch,omitempty"`
}

func (g *GovernanceClient) AnchorTerm(ctx context.Context, entry domain.ControlPlaneSIEMReceiverTermHistory) (domain.ControlPlaneSIEMTermLedgerAnchor, error) {
	anchor, _, err := g.AnchorTermWithReplicaReceipts(ctx, entry)
	return anchor, err
}

func (g *GovernanceClient) AnchorTermWithReplicaReceipts(
	ctx context.Context,
	entry domain.ControlPlaneSIEMReceiverTermHistory,
) (domain.ControlPlaneSIEMTermLedgerAnchor, []domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	if !g.HAEnabled() {
		anchor, err := g.anchorTermLegacy(ctx, entry)
		return anchor, nil, err
	}
	return g.anchorTermHA(ctx, entry)
}

func (g *GovernanceClient) anchorTermLegacy(ctx context.Context, entry domain.ControlPlaneSIEMReceiverTermHistory) (domain.ControlPlaneSIEMTermLedgerAnchor, error) {
	payload := termAnchorPayload{
		Version:        "taskflow-siem-term-anchor-v1",
		Term:           entry.Term,
		RecordHash:     entry.RecordHash,
		PreviousHash:   entry.PreviousHash,
		ActiveReceiver: entry.ActiveReceiver,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	signature, err := g.signer.Sign(body)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	algorithm := "unknown"
	if provider, ok := g.signer.(SignatureAlgorithmProvider); ok {
		algorithm = provider.Algorithm()
	}
	envelopeBody, err := json.Marshal(governanceSignedEnvelope{Payload: string(body), KeyID: g.signer.KeyID(), Algorithm: algorithm, Signature: signature})
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	responseBody, err := g.post(ctx, "/anchors", envelopeBody)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	var receipt struct {
		AnchorID    string `json:"anchor_id"`
		Term        uint64 `json:"term"`
		RecordHash  string `json:"record_hash"`
		ReceiptHash string `json:"receipt_hash"`
		AnchoredAt  string `json:"anchored_at"`
	}
	if err := json.Unmarshal(responseBody, &receipt); err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("decode governance anchor receipt: %w", err)
	}
	if receipt.AnchorID == "" || receipt.Term != entry.Term || receipt.RecordHash != entry.RecordHash || len(receipt.ReceiptHash) != 64 {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("governance anchor receipt identity mismatch")
	}
	receiptIdentity, err := json.Marshal(map[string]any{
		"anchor_id":   receipt.AnchorID,
		"record_hash": receipt.RecordHash,
		"term":        receipt.Term,
	})
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	if governanceReceiptHash(receiptIdentity) != receipt.ReceiptHash {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("governance anchor receipt hash mismatch")
	}
	anchoredAt, err := time.Parse(time.RFC3339Nano, receipt.AnchoredAt)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("parse governance anchor timestamp: %w", err)
	}
	return domain.ControlPlaneSIEMTermLedgerAnchor{
		Term:        entry.Term,
		RecordHash:  entry.RecordHash,
		AnchorID:    receipt.AnchorID,
		KeyID:       g.signer.KeyID(),
		Algorithm:   algorithm,
		Signature:   signature,
		ReceiptHash: receipt.ReceiptHash,
		AnchoredAt:  anchoredAt.UTC(),
	}, nil
}

func (g *GovernanceClient) anchorTermHA(
	ctx context.Context,
	entry domain.ControlPlaneSIEMReceiverTermHistory,
) (domain.ControlPlaneSIEMTermLedgerAnchor, []domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	payload := termAnchorPayload{
		Version:        "taskflow-siem-term-anchor-v1",
		Term:           entry.Term,
		RecordHash:     entry.RecordHash,
		PreviousHash:   entry.PreviousHash,
		ActiveReceiver: entry.ActiveReceiver,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, nil, err
	}
	signature, err := g.signer.Sign(body)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, nil, err
	}
	algorithm := "unknown"
	if provider, ok := g.signer.(SignatureAlgorithmProvider); ok {
		algorithm = provider.Algorithm()
	}
	envelopeBody, err := json.Marshal(governanceSignedEnvelope{Payload: string(body), KeyID: g.signer.KeyID(), Algorithm: algorithm, Signature: signature})
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, nil, err
	}

	verified := make([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, 0, len(g.replicas))
	var canonical governanceAnchorReceipt
	var canonicalSet bool
	transientErrors := make([]string, 0, len(g.replicas))
	for _, replica := range g.replicas {
		responseBody, postErr := g.postReplica(ctx, replica, "/anchors", envelopeBody)
		if postErr != nil {
			var endpointErr *governanceEndpointError
			if errors.As(postErr, &endpointErr) && endpointErr.StatusCode == http.StatusConflict {
				return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, &GovernanceConsistencyError{
					Kind: "anchor", ReplicaID: replica.id, Reason: postErr.Error(),
				}
			}
			transientErrors = append(transientErrors, fmt.Sprintf("%s=%v", replica.id, postErr))
			continue
		}
		var receipt governanceAnchorReceipt
		if err := json.Unmarshal(responseBody, &receipt); err != nil {
			return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, &GovernanceConsistencyError{
				Kind: "anchor", ReplicaID: replica.id, Reason: fmt.Sprintf("decode signed receipt: %v", err),
			}
		}
		keyState, err := verifyGovernanceAnchorReplicaReceipt(replica, receipt, entry, g.membershipEpoch)
		if err != nil {
			return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, &GovernanceConsistencyError{
				Kind: "anchor", ReplicaID: replica.id, Reason: err.Error(),
			}
		}
		if canonicalSet {
			if receipt.AnchorID != canonical.AnchorID || receipt.ReceiptHash != canonical.ReceiptHash || receipt.RecordHash != canonical.RecordHash || receipt.Term != canonical.Term {
				return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, &GovernanceConsistencyError{
					Kind: "anchor", ReplicaID: replica.id, Reason: "signed receipt disagrees with another verified governance replica",
				}
			}
		} else {
			canonical = receipt
			canonicalSet = true
		}
		receivedAt := time.Now().UTC().Truncate(time.Millisecond)
		verified = append(verified, domain.ControlPlaneSIEMGovernanceReplicaReceipt{
			Kind:                 "anchor",
			TargetID:             entry.ID,
			ReplicaID:            replica.id,
			Identity:             entry.RecordHash,
			RemoteID:             receipt.AnchorID,
			ReceiptHash:          receipt.ReceiptHash,
			ReceiptKeyID:         receipt.ReceiptKeyID,
			ReceiptKeyGeneration: receipt.ReceiptKeyGeneration,
			ReceiptKeyState:      keyState,
			ReceiptAlgorithm:     receipt.ReceiptAlgorithm,
			ReceiptSignature:     receipt.ReceiptSignature,
			MembershipEpoch:      receipt.MembershipEpoch,
			ReceivedAt:           receivedAt,
			CreatedAt:            receivedAt,
		})
	}
	if len(verified) < g.Quorum() {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, fmt.Errorf(
			"governance anchor receipt quorum incomplete: verified=%d required=%d errors=%s",
			len(verified), g.Quorum(), strings.Join(transientErrors, "; "),
		)
	}
	anchoredAt, err := time.Parse(time.RFC3339Nano, canonical.AnchoredAt)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, verified, &GovernanceConsistencyError{
			Kind: "anchor", ReplicaID: canonical.ReplicaID, Reason: fmt.Sprintf("parse anchored_at: %v", err),
		}
	}
	return domain.ControlPlaneSIEMTermLedgerAnchor{
		Term:        entry.Term,
		RecordHash:  entry.RecordHash,
		AnchorID:    canonical.AnchorID,
		KeyID:       g.signer.KeyID(),
		Algorithm:   algorithm,
		Signature:   signature,
		ReceiptHash: canonical.ReceiptHash,
		AnchoredAt:  anchoredAt.UTC(),
	}, verified, nil
}

func (g *GovernanceClient) AlertIncident(ctx context.Context, evidence domain.ControlPlaneSIEMIncidentEvidence) error {
	_, err := g.AlertIncidentWithReplicaReceipts(ctx, evidence)
	return err
}

func (g *GovernanceClient) AlertIncidentWithReplicaReceipts(
	ctx context.Context,
	evidence domain.ControlPlaneSIEMIncidentEvidence,
) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	if !g.HAEnabled() {
		return nil, g.alertIncidentLegacy(ctx, evidence)
	}
	return g.alertIncidentHA(ctx, evidence)
}

func (g *GovernanceClient) alertIncidentLegacy(ctx context.Context, evidence domain.ControlPlaneSIEMIncidentEvidence) error {
	payload := struct {
		Version        string  `json:"version"`
		IncidentID     string  `json:"incident_id"`
		Kind           string  `json:"kind"`
		Severity       string  `json:"severity"`
		Term           uint64  `json:"term"`
		ActiveReceiver string  `json:"active_receiver"`
		RPOSequences   uint64  `json:"rpo_sequences"`
		RTOSeconds     float64 `json:"rto_seconds"`
		Reason         string  `json:"reason"`
		EvidenceHash   string  `json:"evidence_hash"`
	}{"taskflow-siem-incident-v1", evidence.IncidentID, evidence.Kind, evidence.Severity, evidence.Term, evidence.ActiveReceiver, evidence.RPOSequences, evidence.RTOSeconds, evidence.Reason, evidence.EvidenceHash}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	signature, err := g.signer.Sign(body)
	if err != nil {
		return err
	}
	algorithm := "unknown"
	if provider, ok := g.signer.(SignatureAlgorithmProvider); ok {
		algorithm = provider.Algorithm()
	}
	envelopeBody, err := json.Marshal(governanceSignedEnvelope{Payload: string(body), KeyID: g.signer.KeyID(), Algorithm: algorithm, Signature: signature})
	if err != nil {
		return err
	}
	responseBody, err := g.post(ctx, "/alerts", envelopeBody)
	if err != nil {
		return err
	}
	var receipt struct {
		Status       string `json:"status"`
		EvidenceHash string `json:"evidence_hash"`
	}
	if err := json.Unmarshal(responseBody, &receipt); err != nil {
		return fmt.Errorf("decode governance incident receipt: %w", err)
	}
	if receipt.Status != "accepted" || receipt.EvidenceHash != evidence.EvidenceHash {
		return fmt.Errorf("governance incident receipt identity mismatch")
	}
	return nil
}

func (g *GovernanceClient) alertIncidentHA(
	ctx context.Context,
	evidence domain.ControlPlaneSIEMIncidentEvidence,
) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	payload := struct {
		Version        string  `json:"version"`
		IncidentID     string  `json:"incident_id"`
		Kind           string  `json:"kind"`
		Severity       string  `json:"severity"`
		Term           uint64  `json:"term"`
		ActiveReceiver string  `json:"active_receiver"`
		RPOSequences   uint64  `json:"rpo_sequences"`
		RTOSeconds     float64 `json:"rto_seconds"`
		Reason         string  `json:"reason"`
		EvidenceHash   string  `json:"evidence_hash"`
	}{"taskflow-siem-incident-v1", evidence.IncidentID, evidence.Kind, evidence.Severity, evidence.Term, evidence.ActiveReceiver, evidence.RPOSequences, evidence.RTOSeconds, evidence.Reason, evidence.EvidenceHash}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	signature, err := g.signer.Sign(body)
	if err != nil {
		return nil, err
	}
	algorithm := "unknown"
	if provider, ok := g.signer.(SignatureAlgorithmProvider); ok {
		algorithm = provider.Algorithm()
	}
	envelopeBody, err := json.Marshal(governanceSignedEnvelope{Payload: string(body), KeyID: g.signer.KeyID(), Algorithm: algorithm, Signature: signature})
	if err != nil {
		return nil, err
	}

	verified := make([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, 0, len(g.replicas))
	canonicalHash := ""
	transientErrors := make([]string, 0, len(g.replicas))
	for _, replica := range g.replicas {
		responseBody, postErr := g.postReplica(ctx, replica, "/alerts", envelopeBody)
		if postErr != nil {
			var endpointErr *governanceEndpointError
			if errors.As(postErr, &endpointErr) && endpointErr.StatusCode == http.StatusConflict {
				return verified, &GovernanceConsistencyError{Kind: "alert", ReplicaID: replica.id, Reason: postErr.Error()}
			}
			transientErrors = append(transientErrors, fmt.Sprintf("%s=%v", replica.id, postErr))
			continue
		}
		var receipt governanceAlertReceipt
		if err := json.Unmarshal(responseBody, &receipt); err != nil {
			return verified, &GovernanceConsistencyError{
				Kind: "alert", ReplicaID: replica.id, Reason: fmt.Sprintf("decode signed receipt: %v", err),
			}
		}
		keyState, err := verifyGovernanceAlertReplicaReceipt(replica, receipt, evidence, g.membershipEpoch)
		if err != nil {
			return verified, &GovernanceConsistencyError{Kind: "alert", ReplicaID: replica.id, Reason: err.Error()}
		}
		if canonicalHash != "" && receipt.ReceiptHash != canonicalHash {
			return verified, &GovernanceConsistencyError{
				Kind: "alert", ReplicaID: replica.id, Reason: "signed receipt disagrees with another verified governance replica",
			}
		}
		canonicalHash = receipt.ReceiptHash
		receivedAt := time.Now().UTC().Truncate(time.Millisecond)
		verified = append(verified, domain.ControlPlaneSIEMGovernanceReplicaReceipt{
			Kind:                 "alert",
			TargetID:             evidence.ID,
			ReplicaID:            replica.id,
			Identity:             evidence.EvidenceHash,
			RemoteID:             evidence.EvidenceHash,
			ReceiptHash:          receipt.ReceiptHash,
			ReceiptKeyID:         receipt.ReceiptKeyID,
			ReceiptKeyGeneration: receipt.ReceiptKeyGeneration,
			ReceiptKeyState:      keyState,
			ReceiptAlgorithm:     receipt.ReceiptAlgorithm,
			ReceiptSignature:     receipt.ReceiptSignature,
			MembershipEpoch:      receipt.MembershipEpoch,
			ReceivedAt:           receivedAt,
			CreatedAt:            receivedAt,
		})
	}
	if len(verified) < g.Quorum() {
		return verified, fmt.Errorf(
			"governance alert receipt quorum incomplete: verified=%d required=%d errors=%s",
			len(verified), g.Quorum(), strings.Join(transientErrors, "; "),
		)
	}
	return verified, nil
}

func (g *GovernanceClient) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("governance endpoint %s returned HTTP %d", path, resp.StatusCode)
	}
	return responseBody, nil
}

type governanceEndpointError struct {
	ReplicaID  string
	Path       string
	StatusCode int
}

func (e *governanceEndpointError) Error() string {
	return fmt.Sprintf("governance replica %s endpoint %s returned HTTP %d", e.ReplicaID, e.Path, e.StatusCode)
}

func (g *GovernanceClient) postReplica(ctx context.Context, replica governanceReplica, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, replica.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if replica.token != "" {
		req.Header.Set("Authorization", "Bearer "+replica.token)
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &governanceEndpointError{ReplicaID: replica.id, Path: path, StatusCode: resp.StatusCode}
	}
	return responseBody, nil
}

func verifyGovernanceAnchorReplicaReceipt(
	replica governanceReplica,
	receipt governanceAnchorReceipt,
	entry domain.ControlPlaneSIEMReceiverTermHistory,
	expectedMembershipEpoch uint64,
) (string, error) {
	if receipt.ReplicaID != replica.id {
		return "", fmt.Errorf("receipt replica id mismatch got=%q want=%q", receipt.ReplicaID, replica.id)
	}
	if receipt.ReceiptAlgorithm != "hmac-sha256" {
		return "", fmt.Errorf("receipt key metadata mismatch")
	}
	verification, err := governanceReceiptVerificationKey(replica, receipt.ReceiptKeyID, time.Now().UTC())
	if err != nil {
		return "", err
	}
	if receipt.ReceiptKeyGeneration != verification.generation {
		return "", fmt.Errorf("receipt key generation mismatch got=%d want=%d", receipt.ReceiptKeyGeneration, verification.generation)
	}
	if receipt.MembershipEpoch != expectedMembershipEpoch {
		return "", fmt.Errorf("receipt membership epoch mismatch got=%d want=%d", receipt.MembershipEpoch, expectedMembershipEpoch)
	}
	if receipt.AnchorID == "" || receipt.Term != entry.Term || receipt.RecordHash != entry.RecordHash || len(receipt.ReceiptHash) != 64 {
		return "", fmt.Errorf("receipt identity mismatch")
	}
	receiptIdentity, err := json.Marshal(map[string]any{
		"anchor_id":   receipt.AnchorID,
		"record_hash": receipt.RecordHash,
		"term":        receipt.Term,
	})
	if err != nil {
		return "", err
	}
	if governanceReceiptHash(receiptIdentity) != receipt.ReceiptHash {
		return "", fmt.Errorf("receipt hash mismatch")
	}
	if _, err := time.Parse(time.RFC3339Nano, receipt.AnchoredAt); err != nil {
		return "", fmt.Errorf("receipt timestamp invalid: %w", err)
	}
	signedPayload, err := json.Marshal(map[string]any{
		"anchor_id":              receipt.AnchorID,
		"anchored_at":            receipt.AnchoredAt,
		"membership_epoch":       receipt.MembershipEpoch,
		"record_hash":            receipt.RecordHash,
		"receipt_hash":           receipt.ReceiptHash,
		"receipt_key_generation": receipt.ReceiptKeyGeneration,
		"replica_id":             receipt.ReplicaID,
		"term":                   receipt.Term,
	})
	if err != nil {
		return "", err
	}
	if !verifyGovernanceReceiptHMAC(verification.key, signedPayload, receipt.ReceiptSignature) {
		return "", fmt.Errorf("receipt signature mismatch")
	}
	return verification.state, nil
}

func verifyGovernanceAlertReplicaReceipt(
	replica governanceReplica,
	receipt governanceAlertReceipt,
	evidence domain.ControlPlaneSIEMIncidentEvidence,
	expectedMembershipEpoch uint64,
) (string, error) {
	if receipt.ReplicaID != replica.id {
		return "", fmt.Errorf("receipt replica id mismatch got=%q want=%q", receipt.ReplicaID, replica.id)
	}
	if receipt.ReceiptAlgorithm != "hmac-sha256" {
		return "", fmt.Errorf("receipt key metadata mismatch")
	}
	verification, err := governanceReceiptVerificationKey(replica, receipt.ReceiptKeyID, time.Now().UTC())
	if err != nil {
		return "", err
	}
	if receipt.ReceiptKeyGeneration != verification.generation {
		return "", fmt.Errorf("receipt key generation mismatch got=%d want=%d", receipt.ReceiptKeyGeneration, verification.generation)
	}
	if receipt.MembershipEpoch != expectedMembershipEpoch {
		return "", fmt.Errorf("receipt membership epoch mismatch got=%d want=%d", receipt.MembershipEpoch, expectedMembershipEpoch)
	}
	if receipt.Status != "accepted" || receipt.EvidenceHash != evidence.EvidenceHash || len(receipt.ReceiptHash) != 64 {
		return "", fmt.Errorf("receipt identity mismatch")
	}
	receiptIdentity, err := json.Marshal(map[string]any{
		"evidence_hash": receipt.EvidenceHash,
		"status":        receipt.Status,
	})
	if err != nil {
		return "", err
	}
	if governanceReceiptHash(receiptIdentity) != receipt.ReceiptHash {
		return "", fmt.Errorf("receipt hash mismatch")
	}
	signedPayload, err := json.Marshal(map[string]any{
		"evidence_hash":          receipt.EvidenceHash,
		"membership_epoch":       receipt.MembershipEpoch,
		"receipt_hash":           receipt.ReceiptHash,
		"receipt_key_generation": receipt.ReceiptKeyGeneration,
		"replica_id":             receipt.ReplicaID,
		"status":                 receipt.Status,
	})
	if err != nil {
		return "", err
	}
	if !verifyGovernanceReceiptHMAC(verification.key, signedPayload, receipt.ReceiptSignature) {
		return "", fmt.Errorf("receipt signature mismatch")
	}
	return verification.state, nil
}

type governanceReceiptVerification struct {
	key        []byte
	generation uint64
	state      string
}

func governanceReceiptVerificationKey(replica governanceReplica, receiptKeyID string, now time.Time) (governanceReceiptVerification, error) {
	receiptKeyID = strings.TrimSpace(receiptKeyID)
	switch receiptKeyID {
	case replica.receiptKeyID:
		return governanceReceiptVerification{key: replica.receiptHMACKey, generation: replica.receiptKeyGeneration, state: "active"}, nil
	case replica.previousReceiptKeyID:
		if replica.previousReceiptKeyID != "" && len(replica.previousReceiptHMACKey) > 0 {
			if replica.previousReceiptValidUntil.IsZero() || now.After(replica.previousReceiptValidUntil) {
				return governanceReceiptVerification{}, fmt.Errorf(
					"previous receipt key grace expired replica=%q key_id=%q valid_until=%s",
					replica.id, receiptKeyID, replica.previousReceiptValidUntil.UTC().Format(time.RFC3339Nano),
				)
			}
			return governanceReceiptVerification{key: replica.previousReceiptHMACKey, generation: replica.previousReceiptKeyGeneration, state: "previous"}, nil
		}
	}
	return governanceReceiptVerification{}, fmt.Errorf("receipt key id %q is not active or previous for replica=%q", receiptKeyID, replica.id)
}

func verifyGovernanceReceiptHMAC(key, payload []byte, signature string) bool {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

func governanceReceiptHash(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
