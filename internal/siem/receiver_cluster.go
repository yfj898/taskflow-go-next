package siem

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type ReceiverClusterAuditRepository interface {
	GetControlPlaneAuditBySequence(context.Context, uint64) (domain.ControlPlaneAudit, error)
}

type ReceiverClusterGuardConfig struct {
	PrimaryCheckpointEndpoint   string
	PrimaryReceiver             string
	SecondaryCheckpointEndpoint string
	SecondaryReceiver           string
	BearerToken                 string
	PageSize                    int
	HTTPTimeout                 time.Duration
	SignatureVerifier           *ReceiverCheckpointSignatureVerifier
}

type ReceiverClusterGuard struct {
	repo   ReceiverClusterAuditRepository
	config ReceiverClusterGuardConfig
	client *http.Client
}

type ReceiverClusterState struct {
	PrimaryHighWatermark   uint64
	SecondaryHighWatermark uint64
	PreferSecondary        bool
}

func NewReceiverClusterGuard(repo ReceiverClusterAuditRepository, config ReceiverClusterGuardConfig) (*ReceiverClusterGuard, error) {
	if repo == nil {
		return nil, fmt.Errorf("receiver cluster audit repository is nil")
	}
	if strings.TrimSpace(config.PrimaryCheckpointEndpoint) == "" || strings.TrimSpace(config.SecondaryCheckpointEndpoint) == "" {
		return nil, fmt.Errorf("receiver cluster checkpoint endpoints are required")
	}
	if strings.TrimSpace(config.PrimaryReceiver) == "" || strings.TrimSpace(config.SecondaryReceiver) == "" {
		return nil, fmt.Errorf("receiver cluster ids are required")
	}
	if config.SignatureVerifier == nil {
		return nil, fmt.Errorf("receiver cluster checkpoint signature verifier is required")
	}
	if config.PageSize <= 0 || config.PageSize > 500 {
		config.PageSize = 500
	}
	if config.HTTPTimeout <= 0 {
		config.HTTPTimeout = 5 * time.Second
	}
	return &ReceiverClusterGuard{
		repo:   repo,
		config: config,
		client: &http.Client{Timeout: config.HTTPTimeout},
	}, nil
}

func (g *ReceiverClusterGuard) Verify(ctx context.Context) (ReceiverClusterState, error) {
	var state ReceiverClusterState
	primary, primaryHead, err := g.loadLedger(ctx, g.config.PrimaryCheckpointEndpoint, g.config.PrimaryReceiver)
	if err != nil {
		return state, fmt.Errorf("verify primary receiver checkpoint: %w", err)
	}
	secondary, secondaryHead, err := g.loadLedger(ctx, g.config.SecondaryCheckpointEndpoint, g.config.SecondaryReceiver)
	if err != nil {
		return state, fmt.Errorf("verify secondary receiver checkpoint: %w", err)
	}
	state.PrimaryHighWatermark = primaryHead.HighWatermarkSequence
	state.SecondaryHighWatermark = secondaryHead.HighWatermarkSequence
	state.PreferSecondary = secondaryHead.HighWatermarkSequence > primaryHead.HighWatermarkSequence
	for sequence, primaryReceipt := range primary {
		secondaryReceipt, ok := secondary[sequence]
		if !ok {
			continue
		}
		if primaryReceipt.RecordHash != secondaryReceipt.RecordHash ||
			primaryReceipt.IdempotencyKey != secondaryReceipt.IdempotencyKey ||
			!strings.EqualFold(primaryReceipt.PayloadSHA256, secondaryReceipt.PayloadSHA256) {
			return state, fmt.Errorf("receiver split-brain divergence at sequence=%d", sequence)
		}
	}
	if primaryHead.HighWatermarkSequence == secondaryHead.HighWatermarkSequence &&
		primaryHead.HighWatermarkSequence > 0 &&
		primaryHead.HeadRecordHash != secondaryHead.HeadRecordHash {
		return state, fmt.Errorf("receiver split-brain head divergence at sequence=%d", primaryHead.HighWatermarkSequence)
	}
	return state, nil
}

func (g *ReceiverClusterGuard) loadLedger(ctx context.Context, endpoint, expectedReceiver string) (map[uint64]receiverCheckpointReceipt, receiverCheckpointResponse, error) {
	ledger := make(map[uint64]receiverCheckpointReceipt)
	after := uint64(0)
	var latest receiverCheckpointResponse
	for {
		page, err := fetchReceiverCheckpoint(
			ctx,
			g.client,
			endpoint,
			g.config.BearerToken,
			after,
			g.config.PageSize,
			g.config.SignatureVerifier,
			expectedReceiver,
		)
		if err != nil {
			return nil, latest, err
		}
		if page.Version != "taskflow-control-plane-siem-checkpoint-v1" {
			return nil, latest, fmt.Errorf("unsupported checkpoint version %q", page.Version)
		}
		latest = page
		if page.HighWatermarkSequence > 0 {
			audit, err := g.repo.GetControlPlaneAuditBySequence(ctx, page.HighWatermarkSequence)
			if err != nil {
				return nil, latest, fmt.Errorf("load local audit for checkpoint head sequence=%d: %w", page.HighWatermarkSequence, err)
			}
			if audit.RecordHash != strings.TrimSpace(page.HeadRecordHash) {
				return nil, latest, fmt.Errorf("receiver checkpoint head diverges from local audit at sequence=%d", page.HighWatermarkSequence)
			}
		}
		if len(page.Items) == 0 {
			if after < page.HighWatermarkSequence {
				return nil, latest, fmt.Errorf("receiver checkpoint omitted receipts after sequence=%d before high watermark=%d", after, page.HighWatermarkSequence)
			}
			return ledger, latest, nil
		}
		for _, item := range page.Items {
			if item.ChainSequence <= after {
				return nil, latest, fmt.Errorf("receiver checkpoint sequence is not strictly increasing: %d after %d", item.ChainSequence, after)
			}
			if err := g.validateReceipt(ctx, item); err != nil {
				return nil, latest, err
			}
			ledger[item.ChainSequence] = item
			after = item.ChainSequence
		}
		if after >= page.HighWatermarkSequence {
			return ledger, latest, nil
		}
	}
}

func (g *ReceiverClusterGuard) validateReceipt(ctx context.Context, item receiverCheckpointReceipt) error {
	audit, err := g.repo.GetControlPlaneAuditBySequence(ctx, item.ChainSequence)
	if err != nil {
		return fmt.Errorf("load local audit sequence=%d for receiver cluster guard: %w", item.ChainSequence, err)
	}
	computedHash, err := domain.ComputeControlPlaneAuditHash(audit)
	if err != nil {
		return fmt.Errorf("compute local audit hash sequence=%d for receiver cluster guard: %w", item.ChainSequence, err)
	}
	if item.RecordHash != audit.RecordHash || computedHash != audit.RecordHash {
		return fmt.Errorf("receiver checkpoint record hash diverges at sequence=%d", item.ChainSequence)
	}
	_, idempotencyKey, payloadSHA, err := canonicalDeliveryIdentityFromAudit(audit)
	if err != nil {
		return fmt.Errorf("build local canonical delivery sequence=%d: %w", item.ChainSequence, err)
	}
	if item.IdempotencyKey != idempotencyKey || !strings.EqualFold(item.PayloadSHA256, payloadSHA) {
		return fmt.Errorf("receiver checkpoint payload identity diverges at sequence=%d", item.ChainSequence)
	}
	return nil
}
