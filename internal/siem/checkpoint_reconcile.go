package siem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type CheckpointReconciliationRepository interface {
	GetControlPlaneAuditBySequence(context.Context, uint64) (domain.ControlPlaneAudit, error)
	ImportControlPlaneAuditDeliveryReceipt(context.Context, domain.ControlPlaneAuditDeliveryReceipt) (bool, error)
}

type CheckpointReconcilerConfig struct {
	Endpoint          string
	BearerToken       string
	HTTPTimeout       time.Duration
	PageSize          int
	EvidenceVerifier  ReceiptEvidenceVerifier
	MinEvidenceCopies int
	SignatureVerifier *ReceiverCheckpointSignatureVerifier
	ExpectedReceiver  string
}

type CheckpointReconciler struct {
	repo   CheckpointReconciliationRepository
	config CheckpointReconcilerConfig
	client *http.Client
}

type receiverCheckpointReceipt struct {
	ChainSequence  uint64 `json:"chain_sequence"`
	RecordHash     string `json:"record_hash"`
	IdempotencyKey string `json:"idempotency_key"`
	PayloadSHA256  string `json:"payload_sha256"`
	ReceiptID      string `json:"receipt_id"`
	ReceiverTerm   uint64 `json:"receiver_term,omitempty"`
	AcceptedAt     string `json:"accepted_at"`
}

type receiverCheckpointResponse struct {
	Version               string                      `json:"version"`
	ReceiverID            string                      `json:"receiver_id,omitempty"`
	ReceiverGeneration    uint64                      `json:"receiver_generation,omitempty"`
	ReceiverTerm          uint64                      `json:"receiver_term,omitempty"`
	ActiveReceiver        string                      `json:"active_receiver,omitempty"`
	HighWatermarkSequence uint64                      `json:"high_watermark_sequence"`
	HeadRecordHash        string                      `json:"head_record_hash"`
	Items                 []receiverCheckpointReceipt `json:"items"`
}

func NewCheckpointReconciler(repo CheckpointReconciliationRepository, config CheckpointReconcilerConfig) (*CheckpointReconciler, error) {
	if repo == nil {
		return nil, errors.New("SIEM checkpoint reconciliation repository is nil")
	}
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if config.Endpoint == "" {
		return nil, errors.New("SIEM checkpoint endpoint is empty")
	}
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("SIEM checkpoint endpoint must be an absolute http(s) URL")
	}
	if config.HTTPTimeout <= 0 {
		config.HTTPTimeout = 5 * time.Second
	}
	if config.PageSize <= 0 || config.PageSize > 500 {
		config.PageSize = 500
	}
	if config.MinEvidenceCopies <= 0 {
		config.MinEvidenceCopies = 1
	}
	return &CheckpointReconciler{repo: repo, config: config, client: &http.Client{Timeout: config.HTTPTimeout}}, nil
}

func (r *CheckpointReconciler) Reconcile(ctx context.Context) (int, error) {
	after := uint64(0)
	imported := 0
	for {
		page, err := r.fetch(ctx, after)
		if err != nil {
			return imported, err
		}
		if page.Version != "taskflow-control-plane-siem-checkpoint-v1" {
			return imported, fmt.Errorf("unsupported SIEM checkpoint version %q", page.Version)
		}
		if page.HighWatermarkSequence > 0 {
			audit, err := r.repo.GetControlPlaneAuditBySequence(ctx, page.HighWatermarkSequence)
			if err != nil {
				return imported, fmt.Errorf("load local audit for receiver high watermark %d: %w", page.HighWatermarkSequence, err)
			}
			if audit.RecordHash != strings.TrimSpace(page.HeadRecordHash) {
				return imported, fmt.Errorf("receiver checkpoint high watermark sequence=%d record hash mismatch", page.HighWatermarkSequence)
			}
		}
		if len(page.Items) == 0 {
			return imported, nil
		}
		for _, item := range page.Items {
			if item.ChainSequence <= after {
				return imported, fmt.Errorf("receiver checkpoint sequence is not strictly increasing: %d after %d", item.ChainSequence, after)
			}
			created, err := r.reconcileReceipt(ctx, item, page.ReceiverID, page.ReceiverGeneration, page.ReceiverTerm)
			if err != nil {
				return imported, err
			}
			if created {
				imported++
			}
			after = item.ChainSequence
		}
		if len(page.Items) < r.config.PageSize || after >= page.HighWatermarkSequence {
			return imported, nil
		}
	}
}

func (r *CheckpointReconciler) fetch(ctx context.Context, after uint64) (receiverCheckpointResponse, error) {
	return fetchReceiverCheckpoint(
		ctx,
		r.client,
		r.config.Endpoint,
		r.config.BearerToken,
		after,
		r.config.PageSize,
		r.config.SignatureVerifier,
		r.config.ExpectedReceiver,
	)
}

func (r *CheckpointReconciler) reconcileReceipt(ctx context.Context, item receiverCheckpointReceipt, receiverID string, receiverGeneration, receiverTerm uint64) (bool, error) {
	item.RecordHash = strings.TrimSpace(item.RecordHash)
	item.IdempotencyKey = strings.TrimSpace(item.IdempotencyKey)
	item.PayloadSHA256 = strings.ToLower(strings.TrimSpace(item.PayloadSHA256))
	item.ReceiptID = strings.TrimSpace(item.ReceiptID)
	if item.ReceiverTerm > 0 {
		receiverTerm = item.ReceiverTerm
	}
	if item.ChainSequence == 0 || len(item.RecordHash) != 64 || len(item.PayloadSHA256) != 64 || item.IdempotencyKey == "" || item.ReceiptID == "" {
		return false, fmt.Errorf("receiver checkpoint contains invalid receipt sequence=%d", item.ChainSequence)
	}
	audit, err := r.repo.GetControlPlaneAuditBySequence(ctx, item.ChainSequence)
	if err != nil {
		return false, fmt.Errorf("load local audit sequence=%d: %w", item.ChainSequence, err)
	}
	computedRecordHash, err := domain.ComputeControlPlaneAuditHash(audit)
	if err != nil {
		return false, fmt.Errorf("compute local audit hash sequence=%d: %w", item.ChainSequence, err)
	}
	if audit.RecordHash != item.RecordHash || computedRecordHash != item.RecordHash {
		return false, fmt.Errorf("receiver checkpoint receipt sequence=%d does not match verified local audit", item.ChainSequence)
	}
	_, expectedIdempotencyKey, expectedPayloadSHA, err := canonicalDeliveryIdentityFromAudit(audit)
	if err != nil {
		return false, fmt.Errorf("build canonical SIEM identity sequence=%d: %w", item.ChainSequence, err)
	}
	if expectedIdempotencyKey != item.IdempotencyKey || !strings.EqualFold(expectedPayloadSHA, item.PayloadSHA256) {
		return false, fmt.Errorf("receiver checkpoint receipt sequence=%d canonical payload identity mismatch", item.ChainSequence)
	}
	proof := deliveryReceiptProof{
		IdempotencyKey:     item.IdempotencyKey,
		FirstSequence:      item.ChainSequence,
		LastSequence:       item.ChainSequence,
		PayloadSHA256:      item.PayloadSHA256,
		ReceiptID:          item.ReceiptID,
		ReceiverID:         receiverID,
		ReceiverGeneration: receiverGeneration,
		ReceiverTerm:       receiverTerm,
	}
	replicaCopies := 0
	if r.config.EvidenceVerifier != nil {
		key := deliveryEvidenceObjectKey(item.ChainSequence, item.ChainSequence, item.IdempotencyKey)
		copies, source, err := r.config.EvidenceVerifier.VerifyReceiptEvidence(ctx, key, proof, r.config.MinEvidenceCopies)
		if err != nil {
			return false, fmt.Errorf("verify SIEM recovery evidence sequence=%d: %w", item.ChainSequence, err)
		}
		replicaCopies = copies
		if source != "" {
			appmetrics.ControlPlaneSIEMRecoveryEvidence.WithLabelValues(source).Inc()
		}
	}
	acceptedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.AcceptedAt))
	if err != nil {
		return false, fmt.Errorf("receiver checkpoint receipt sequence=%d invalid accepted_at: %w", item.ChainSequence, err)
	}
	checkpointBytes, err := json.Marshal(item)
	if err != nil {
		return false, err
	}
	checkpointHash := sha256.Sum256(checkpointBytes)
	receipt := domain.ControlPlaneAuditDeliveryReceipt{
		AuditID:        audit.ID,
		ChainSequence:  item.ChainSequence,
		RecordHash:     item.RecordHash,
		ReceiptID:      item.ReceiptID,
		IdempotencyKey: item.IdempotencyKey,
		PayloadSHA256:  item.PayloadSHA256,
		AckHash:        hex.EncodeToString(checkpointHash[:]),
		AckStatus:      "reconciled",
		ReceiverID:     receiverID,
		ReceiverGen:    receiverGeneration,
		ReceiverTerm:   receiverTerm,
		ReplicaCopies:  replicaCopies,
		RecoverySource: "receiver-checkpoint",
		DeliveredAt:    acceptedAt.UTC(),
	}
	created, err := r.repo.ImportControlPlaneAuditDeliveryReceipt(ctx, receipt)
	if err != nil {
		return false, fmt.Errorf("import receiver checkpoint receipt sequence=%d: %w", item.ChainSequence, err)
	}
	if created {
		appmetrics.ControlPlaneSIEMRecoveredReceipts.Inc()
	}
	return created, nil
}
