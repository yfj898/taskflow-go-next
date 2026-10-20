package siem

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type DeliveryRepository interface {
	ActivateControlPlaneSIEMDeliveryFence(context.Context, uint64) (uint64, error)
	ClaimControlPlaneAuditDeliveries(context.Context, time.Time, time.Time, string, uint64, int) ([]domain.ControlPlaneAuditDelivery, error)
	MarkControlPlaneAuditDeliveriesDelivered(context.Context, []uint64, string, uint64, time.Time, domain.ControlPlaneSIEMAcknowledgement) error
	FailControlPlaneAuditDeliveries(context.Context, []domain.ControlPlaneAuditDelivery, string, uint64, time.Time, time.Time, error, int) (int, int, error)
	GetControlPlaneAuditCheckpoint(context.Context) (domain.ControlPlaneAuditChainState, error)
	ControlPlaneAuditDeliveryStats(context.Context) (domain.ControlPlaneAuditDeliveryStats, error)
}

type DeliveryConfig struct {
	Endpoint                              string
	PrimaryReceiver                       string
	PrimaryCheckpointEndpoint             string
	SecondaryEndpoint                     string
	SecondaryReceiver                     string
	SecondaryCheckpointEndpoint           string
	PrimaryTermEndpoint                   string
	SecondaryTermEndpoint                 string
	FailoverEnabled                       bool
	ShadowSecondary                       bool
	StartOnSecondary                      bool
	TermFencingEnabled                    bool
	AntiEntropyEnabled                    bool
	AntiEntropyInterval                   time.Duration
	PromotionWorkflowEnabled              bool
	PromotionIntentTTL                    time.Duration
	PromotionLease                        time.Duration
	PromotionApprovalQuorum               int
	PromotionRolePolicy                   string
	BreakGlassEnabled                     bool
	Governance                            *GovernanceClient
	GovernanceDebtPolicyEnabled           bool
	GovernanceMaxOutstanding              int
	GovernanceMaxAge                      time.Duration
	GovernanceRemediationApprovalRequired bool
	GovernanceRemediationApprovalTTL      time.Duration
	GovernanceRemediationStaleTargetFence bool
	RTOAlertThreshold                     time.Duration
	RPOPolicyEnabled                      bool
	RPOMaxSequences                       uint64
	RPOGrace                              time.Duration
	CheckpointVerifier                    *ReceiverCheckpointSignatureVerifier
	BearerToken                           string
	Interval                              time.Duration
	BatchSize                             int
	LeaseDuration                         time.Duration
	MaxAttempts                           int
	RetryBase                             time.Duration
	RetryMax                              time.Duration
	HTTPTimeout                           time.Duration
	Coordinator                           DeliveryCoordinator
	LeadershipTTL                         time.Duration
	EvidenceReplica                       EvidenceReplica
}

type DeliveryWorker struct {
	repo            DeliveryRepository
	config          DeliveryConfig
	signer          CheckpointSigner
	client          *http.Client
	activeSecondary bool
	lastAntiEntropy time.Time
}

type deliveryEvent struct {
	ChainSequence uint64          `json:"chain_sequence"`
	RecordHash    string          `json:"record_hash"`
	Audit         json.RawMessage `json:"audit"`
}

type deliveryCheckpoint struct {
	BatchFirstSequence      uint64 `json:"batch_first_sequence"`
	BatchLastSequence       uint64 `json:"batch_last_sequence"`
	ChainLastSequence       uint64 `json:"chain_last_sequence"`
	ChainHeadHash           string `json:"chain_head_hash"`
	RetentionAnchorSequence uint64 `json:"retention_anchor_sequence"`
	RetentionAnchorHash     string `json:"retention_anchor_hash"`
	GeneratedAt             string `json:"generated_at"`
}

type deliveryEnvelope struct {
	Version    string             `json:"version"`
	Events     []deliveryEvent    `json:"events"`
	Checkpoint deliveryCheckpoint `json:"checkpoint"`
}

type deliveryAcknowledgement struct {
	Status             string `json:"status"`
	IdempotencyKey     string `json:"idempotency_key"`
	FirstSequence      uint64 `json:"first_sequence"`
	LastSequence       uint64 `json:"last_sequence"`
	PayloadSHA256      string `json:"payload_sha256"`
	ReceiptID          string `json:"receipt_id"`
	ReceiverID         string `json:"receiver_id,omitempty"`
	ReceiverGeneration uint64 `json:"receiver_generation,omitempty"`
	ReceiverTerm       uint64 `json:"receiver_term,omitempty"`
}

// deliveryReceiptProof is the canonical receiver receipt identity persisted in
// DR evidence. It deliberately excludes the transient acknowledgement status:
// a lost HTTP response may turn an initial "accepted" into a later
// "duplicate" acknowledgement for the same logical receipt. Keeping only the
// stable receipt identity makes the immutable evidence object byte-identical
// across safe retries.
type deliveryReceiptProof struct {
	IdempotencyKey     string `json:"idempotency_key"`
	FirstSequence      uint64 `json:"first_sequence"`
	LastSequence       uint64 `json:"last_sequence"`
	PayloadSHA256      string `json:"payload_sha256"`
	ReceiptID          string `json:"receipt_id"`
	ReceiverID         string `json:"receiver_id,omitempty"`
	ReceiverGeneration uint64 `json:"receiver_generation,omitempty"`
	ReceiverTerm       uint64 `json:"receiver_term,omitempty"`
}

type deliveryEvidence struct {
	Version                string                 `json:"version"`
	IdempotencyKey         string                 `json:"idempotency_key"`
	FirstSequence          uint64                 `json:"first_sequence"`
	LastSequence           uint64                 `json:"last_sequence"`
	PayloadSHA256          string                 `json:"payload_sha256"`
	Envelope               json.RawMessage        `json:"envelope"`
	KeyID                  string                 `json:"key_id"`
	Algorithm              string                 `json:"algorithm"`
	Signature              string                 `json:"signature"`
	PreviousKeyID          string                 `json:"previous_key_id,omitempty"`
	PreviousAlgorithm      string                 `json:"previous_algorithm,omitempty"`
	PreviousSignature      string                 `json:"previous_signature,omitempty"`
	ReceiverReceipt        deliveryReceiptProof   `json:"receiver_receipt"`
	ReceiptProofSHA256     string                 `json:"receipt_proof_sha256"`
	ReceiverReceipts       []deliveryReceiptProof `json:"receiver_receipts,omitempty"`
	ReceiverReceiptsSHA256 string                 `json:"receiver_receipts_sha256,omitempty"`
}

func NewDeliveryWorker(repo DeliveryRepository, config DeliveryConfig, signer CheckpointSigner) (*DeliveryWorker, error) {
	if repo == nil {
		return nil, fmt.Errorf("SIEM delivery repository is nil")
	}
	if signer == nil {
		return nil, fmt.Errorf("SIEM checkpoint signer is nil")
	}
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if config.Endpoint == "" {
		return nil, fmt.Errorf("SIEM endpoint is empty")
	}
	config.PrimaryReceiver = strings.TrimSpace(config.PrimaryReceiver)
	config.PrimaryCheckpointEndpoint = strings.TrimSpace(config.PrimaryCheckpointEndpoint)
	config.SecondaryEndpoint = strings.TrimSpace(config.SecondaryEndpoint)
	config.SecondaryReceiver = strings.TrimSpace(config.SecondaryReceiver)
	config.SecondaryCheckpointEndpoint = strings.TrimSpace(config.SecondaryCheckpointEndpoint)
	config.PrimaryTermEndpoint = strings.TrimSpace(config.PrimaryTermEndpoint)
	config.SecondaryTermEndpoint = strings.TrimSpace(config.SecondaryTermEndpoint)
	if config.FailoverEnabled || config.ShadowSecondary {
		if config.SecondaryEndpoint == "" || config.SecondaryReceiver == "" {
			return nil, fmt.Errorf("SIEM secondary endpoint and receiver id are required for failover/shadow delivery")
		}
	}
	if config.FailoverEnabled {
		if config.SecondaryCheckpointEndpoint == "" || config.CheckpointVerifier == nil {
			return nil, fmt.Errorf("SIEM failover requires a signed secondary checkpoint endpoint")
		}
	}
	if config.TermFencingEnabled {
		if config.PrimaryReceiver == "" || config.SecondaryReceiver == "" || config.PrimaryTermEndpoint == "" || config.SecondaryTermEndpoint == "" {
			return nil, fmt.Errorf("SIEM receiver term fencing requires receiver ids and both term endpoints")
		}
	}
	if config.AntiEntropyEnabled && config.AntiEntropyInterval <= 0 {
		config.AntiEntropyInterval = 2 * time.Second
	}
	if config.PromotionWorkflowEnabled {
		if !config.TermFencingEnabled {
			return nil, fmt.Errorf("SIEM promotion workflow requires receiver term fencing")
		}
		if config.PromotionIntentTTL <= 0 {
			config.PromotionIntentTTL = 5 * time.Minute
		}
		if config.PromotionLease <= 0 {
			config.PromotionLease = 15 * time.Second
		}
		if config.PromotionApprovalQuorum <= 0 {
			config.PromotionApprovalQuorum = 1
		}
	}
	if config.RPOPolicyEnabled {
		if !config.TermFencingEnabled {
			return nil, fmt.Errorf("SIEM RPO policy requires receiver term fencing")
		}
		if !config.PromotionWorkflowEnabled {
			return nil, fmt.Errorf("SIEM RPO policy requires coordinated promotion workflow")
		}
		if config.RPOGrace <= 0 {
			config.RPOGrace = 5 * time.Second
		}
	}
	if config.GovernanceDebtPolicyEnabled {
		if config.Governance == nil {
			return nil, fmt.Errorf("SIEM governance debt policy requires governance delivery")
		}
		if config.GovernanceMaxOutstanding <= 0 {
			config.GovernanceMaxOutstanding = 100
		}
		if config.GovernanceMaxAge <= 0 {
			config.GovernanceMaxAge = 5 * time.Minute
		}
	}
	if config.GovernanceRemediationApprovalRequired {
		if config.Governance == nil {
			return nil, fmt.Errorf("SIEM governance remediation approval requires governance delivery")
		}
		if config.GovernanceRemediationApprovalTTL <= 0 {
			config.GovernanceRemediationApprovalTTL = 5 * time.Minute
		}
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.BatchSize <= 0 || config.BatchSize > 500 {
		config.BatchSize = 100
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 8
	}
	if config.RetryBase <= 0 {
		config.RetryBase = time.Second
	}
	if config.RetryMax < config.RetryBase {
		config.RetryMax = time.Minute
	}
	if config.HTTPTimeout <= 0 {
		config.HTTPTimeout = 5 * time.Second
	}
	if config.LeadershipTTL <= 0 {
		config.LeadershipTTL = 15 * time.Second
	}
	return &DeliveryWorker{
		repo:            repo,
		config:          config,
		signer:          signer,
		client:          &http.Client{Timeout: config.HTTPTimeout},
		activeSecondary: config.StartOnSecondary,
	}, nil
}

func (w *DeliveryWorker) Start(ctx context.Context) {
	if w.config.Coordinator != nil {
		go w.coordinatedLoop(ctx)
		return
	}
	go func() {
		w.runOnce(ctx, 0)
		ticker := time.NewTicker(w.config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.runOnce(ctx, 0)
			}
		}
	}()
}

func (w *DeliveryWorker) coordinatedLoop(ctx context.Context) {
	for ctx.Err() == nil {
		lease, acquired, err := w.config.Coordinator.TryAcquire(ctx, w.config.LeadershipTTL)
		if err != nil {
			appmetrics.ControlPlaneSIEMLeadership.WithLabelValues("acquire_failed").Inc()
			log.Printf("acquire control-plane SIEM leadership: %v", err)
			if !sleepContext(ctx, w.config.Interval) {
				return
			}
			continue
		}
		if !acquired {
			appmetrics.ControlPlaneSIEMLeadership.WithLabelValues("follower").Inc()
			if !sleepContext(ctx, w.config.Interval) {
				return
			}
			continue
		}

		candidateFence := lease.Fence()
		fence, err := w.repo.ActivateControlPlaneSIEMDeliveryFence(ctx, candidateFence)
		if err != nil {
			appmetrics.ControlPlaneSIEMLeadership.WithLabelValues("fence_failed").Inc()
			log.Printf("activate control-plane SIEM leader candidate_fence=%d: %v", candidateFence, err)
			lease.Release()
			if !sleepContext(ctx, w.config.Interval) {
				return
			}
			continue
		}
		appmetrics.ControlPlaneSIEMLeader.Set(1)
		appmetrics.ControlPlaneSIEMLeaderFence.Set(float64(fence))
		appmetrics.ControlPlaneSIEMLeadership.WithLabelValues("acquired").Inc()
		w.runLeaderLoop(lease.Context(), fence)
		appmetrics.ControlPlaneSIEMLeader.Set(0)
		appmetrics.ControlPlaneSIEMLeadership.WithLabelValues("lost").Inc()
		lease.Release()
	}
}

func (w *DeliveryWorker) runLeaderLoop(ctx context.Context, fence uint64) {
	w.runOnce(ctx, fence)
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnce(ctx, fence)
		}
	}
}

func (w *DeliveryWorker) runOnce(ctx context.Context, leaderFence uint64) {
	if w.config.AntiEntropyEnabled && (w.lastAntiEntropy.IsZero() || time.Since(w.lastAntiEntropy) >= w.config.AntiEntropyInterval) {
		w.lastAntiEntropy = time.Now()
		if _, err := w.SyncReceiverAntiEntropy(ctx); err != nil {
			appmetrics.ControlPlaneSIEMAntiEntropy.WithLabelValues("failed").Inc()
			log.Printf("control-plane SIEM receiver anti-entropy: %v", err)
		} else {
			appmetrics.ControlPlaneSIEMAntiEntropy.WithLabelValues("success").Inc()
		}
	}
	w.reconcileGovernance(ctx)
	now := time.Now().UTC().Truncate(time.Millisecond)
	randomToken, err := randomLeaseToken()
	if err != nil {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("claim_failed").Inc()
		log.Printf("create control-plane SIEM lease token: %v", err)
		return
	}
	leaseToken := randomToken
	if leaderFence > 0 {
		leaseToken = fmt.Sprintf("%d:%s", leaderFence, randomToken)
	}
	deliveries, err := w.repo.ClaimControlPlaneAuditDeliveries(
		ctx,
		now,
		now.Add(w.config.LeaseDuration),
		leaseToken,
		leaderFence,
		w.config.BatchSize,
	)
	if err != nil {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("claim_failed").Inc()
		log.Printf("claim control-plane SIEM deliveries: %v", err)
		w.refreshStats(ctx)
		return
	}
	if len(deliveries) == 0 {
		w.refreshStats(ctx)
		return
	}
	for _, delivery := range deliveries {
		w.deliverOne(ctx, delivery, leaseToken, leaderFence, now)
	}
	w.refreshStats(ctx)
}

func (w *DeliveryWorker) deliverOne(ctx context.Context, delivery domain.ControlPlaneAuditDelivery, leaseToken string, leaderFence uint64, now time.Time) {
	deliveries := []domain.ControlPlaneAuditDelivery{delivery}
	body, first, last, err := w.buildEnvelope(ctx, deliveries, now)
	idempotencyKey := deliveryIdempotencyKey(deliveries)
	var ack domain.ControlPlaneSIEMAcknowledgement
	var evidence []byte
	if err == nil {
		ack, evidence, err = w.post(ctx, body, first, last, idempotencyKey)
	}
	if err == nil && w.config.EvidenceReplica != nil {
		copies, replicaErr := w.config.EvidenceReplica.Replicate(ctx, deliveryEvidenceObjectKey(first, last, idempotencyKey), evidence)
		ack.ReplicaCopies = copies
		appmetrics.ControlPlaneSIEMDRCopies.Set(float64(copies))
		if replicaErr != nil {
			appmetrics.ControlPlaneSIEMDR.WithLabelValues("insufficient_copies").Inc()
			err = replicaErr
		} else {
			appmetrics.ControlPlaneSIEMDR.WithLabelValues("durable").Inc()
		}
	}
	ids := []uint64{delivery.ID}
	settleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err == nil {
		if settleErr := w.repo.MarkControlPlaneAuditDeliveriesDelivered(settleCtx, ids, leaseToken, leaderFence, time.Now(), ack); settleErr != nil {
			appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("settle_failed").Inc()
			log.Printf("settle control-plane SIEM deliveries: %v", settleErr)
			return
		}
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("delivered").Inc()
		return
	}

	retryAt := now.Add(retryDelay(w.config.RetryBase, w.config.RetryMax, delivery.Attempts))
	retried, dead, settleErr := w.repo.FailControlPlaneAuditDeliveries(
		settleCtx,
		deliveries,
		leaseToken,
		leaderFence,
		now,
		retryAt,
		err,
		w.config.MaxAttempts,
	)
	if settleErr != nil {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("settle_failed").Inc()
		log.Printf("settle failed control-plane SIEM deliveries: %v", settleErr)
		return
	}
	if retried > 0 {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("retry").Add(float64(retried))
	}
	if dead > 0 {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("dlq").Add(float64(dead))
	}
	log.Printf("control-plane SIEM delivery failed: retry=%d dlq=%d err=%v", retried, dead, err)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *DeliveryWorker) buildEnvelope(ctx context.Context, deliveries []domain.ControlPlaneAuditDelivery, now time.Time) ([]byte, uint64, uint64, error) {
	_ = ctx
	_ = now
	return buildCanonicalDeliveryEnvelope(deliveries)
}

func buildCanonicalDeliveryEnvelope(deliveries []domain.ControlPlaneAuditDelivery) ([]byte, uint64, uint64, error) {
	if len(deliveries) == 0 {
		return nil, 0, 0, fmt.Errorf("cannot build empty SIEM delivery envelope")
	}
	events := make([]deliveryEvent, 0, len(deliveries))
	var firstAudit domain.ControlPlaneAudit
	for _, delivery := range deliveries {
		if !json.Valid([]byte(delivery.PayloadJSON)) {
			return nil, 0, 0, fmt.Errorf("delivery %d has invalid audit payload", delivery.ID)
		}
		if len(events) == 0 {
			if err := json.Unmarshal([]byte(delivery.PayloadJSON), &firstAudit); err != nil {
				return nil, 0, 0, fmt.Errorf("decode delivery %d audit payload: %w", delivery.ID, err)
			}
		}
		events = append(events, deliveryEvent{
			ChainSequence: delivery.ChainSequence,
			RecordHash:    delivery.RecordHash,
			Audit:         json.RawMessage(delivery.PayloadJSON),
		})
	}
	first := deliveries[0].ChainSequence
	last := deliveries[len(deliveries)-1].ChainSequence
	generatedAt := time.Time{}
	if firstAudit.CompletedAt != nil {
		generatedAt = firstAudit.CompletedAt.UTC()
	}
	if generatedAt.IsZero() {
		generatedAt = firstAudit.CreatedAt.UTC()
	}
	if generatedAt.IsZero() {
		generatedAt = time.Unix(0, 0).UTC()
	}
	envelope := deliveryEnvelope{
		Version: "taskflow-control-plane-siem-v2",
		Events:  events,
		Checkpoint: deliveryCheckpoint{
			BatchFirstSequence:      first,
			BatchLastSequence:       last,
			ChainLastSequence:       last,
			ChainHeadHash:           deliveries[len(deliveries)-1].RecordHash,
			RetentionAnchorSequence: 0,
			RetentionAnchorHash:     "",
			GeneratedAt:             generatedAt.Format(time.RFC3339Nano),
		},
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("marshal SIEM delivery envelope: %w", err)
	}
	return body, first, last, nil
}

func canonicalDeliveryIdentityFromAudit(audit domain.ControlPlaneAudit) ([]byte, string, string, error) {
	if audit.ChainSequence == nil || *audit.ChainSequence == 0 || strings.TrimSpace(audit.RecordHash) == "" {
		return nil, "", "", fmt.Errorf("audit is not sealed for canonical SIEM delivery")
	}
	if audit.CompletedAt == nil {
		return nil, "", "", fmt.Errorf("audit sequence=%d has no completion time", *audit.ChainSequence)
	}
	payload, err := json.Marshal(audit)
	if err != nil {
		return nil, "", "", fmt.Errorf("marshal audit sequence=%d: %w", *audit.ChainSequence, err)
	}
	delivery := domain.ControlPlaneAuditDelivery{
		AuditID:       audit.ID,
		ChainSequence: *audit.ChainSequence,
		RecordHash:    audit.RecordHash,
		PayloadJSON:   string(payload),
	}
	body, _, _, err := buildCanonicalDeliveryEnvelope([]domain.ControlPlaneAuditDelivery{delivery})
	if err != nil {
		return nil, "", "", err
	}
	digest := sha256.Sum256(body)
	return body, deliveryIdempotencyKey([]domain.ControlPlaneAuditDelivery{delivery}), hex.EncodeToString(digest[:]), nil
}

func (w *DeliveryWorker) post(ctx context.Context, body []byte, first, last uint64, idempotencyKey string) (domain.ControlPlaneSIEMAcknowledgement, []byte, error) {
	term := uint64(1)
	activeSecondary := w.activeSecondary
	if w.config.TermFencingEnabled {
		state, err := w.getReceiverRoutingState(ctx)
		if err != nil {
			return domain.ControlPlaneSIEMAcknowledgement{}, nil, fmt.Errorf("load SIEM receiver routing state: %w", err)
		}
		term = state.Term
		activeSecondary = state.ActiveReceiver == w.config.SecondaryReceiver
		appmetrics.ControlPlaneSIEMReceiverTerm.Set(float64(term))
	}
	if activeSecondary {
		result, evidence, err := w.postEndpoint(ctx, w.config.SecondaryEndpoint, w.config.SecondaryReceiver, term, body, first, last, idempotencyKey)
		if err != nil {
			appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("secondary_active_failed").Inc()
			return result, evidence, err
		}
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("secondary_active_delivered").Inc()
		return result, evidence, nil
	}
	result, evidence, primaryErr := w.postEndpoint(ctx, w.config.Endpoint, w.config.PrimaryReceiver, term, body, first, last, idempotencyKey)
	if primaryErr == nil {
		if w.config.ShadowSecondary && w.config.SecondaryEndpoint != "" {
			shadowAck, _, err := w.postEndpoint(ctx, w.config.SecondaryEndpoint, w.config.SecondaryReceiver, term, body, first, last, idempotencyKey)
			if err != nil {
				appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("shadow_failed").Inc()
				log.Printf("shadow control-plane SIEM delivery receiver=%s sequence=%d: %v", w.config.SecondaryReceiver, first, err)
			} else {
				evidence, err = appendReceiverProofToEvidence(evidence, shadowAck, first, last)
				if err != nil {
					return result, nil, fmt.Errorf("record shadow receiver proof: %w", err)
				}
				appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("shadow_delivered").Inc()
			}
		}
		return result, evidence, nil
	}
	if !w.config.FailoverEnabled {
		return result, evidence, primaryErr
	}
	failoverStarted := time.Now()
	if err := w.ensureSecondaryFailoverReady(ctx, first); err != nil {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("failover_fenced").Inc()
		return result, evidence, fmt.Errorf("primary SIEM delivery failed: %v; secondary failover fenced: %w", primaryErr, err)
	}
	if w.config.TermFencingEnabled {
		promoted, promoteErr := w.promoteReceiverTerm(ctx, w.config.SecondaryReceiver, term, "automatic-failover")
		if promoteErr != nil {
			appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("failover_fenced").Inc()
			return result, evidence, fmt.Errorf("primary SIEM delivery failed: %v; promote secondary term: %w", primaryErr, promoteErr)
		}
		term = promoted.Term
	}
	secondaryResult, secondaryEvidence, secondaryErr := w.postEndpoint(ctx, w.config.SecondaryEndpoint, w.config.SecondaryReceiver, term, body, first, last, idempotencyKey)
	if secondaryErr != nil {
		appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("failover_failed").Inc()
		return secondaryResult, secondaryEvidence, fmt.Errorf("primary SIEM delivery failed: %v; secondary SIEM delivery failed: %w", primaryErr, secondaryErr)
	}
	if !w.config.TermFencingEnabled {
		w.activeSecondary = true
	}
	appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("failover_delivered").Inc()
	appmetrics.ControlPlaneSIEMSwitchover.WithLabelValues("secondary", "automatic").Inc()
	appmetrics.ControlPlaneSIEMSwitchoverRTO.Set(time.Since(failoverStarted).Seconds())
	return secondaryResult, secondaryEvidence, nil
}

func (w *DeliveryWorker) ensureSecondaryFailoverReady(ctx context.Context, first uint64) error {
	checkpoint, err := fetchReceiverCheckpoint(
		ctx,
		w.client,
		w.config.SecondaryCheckpointEndpoint,
		w.config.BearerToken,
		0,
		1,
		w.config.CheckpointVerifier,
		w.config.SecondaryReceiver,
	)
	if err != nil {
		return err
	}
	if checkpoint.Version != "taskflow-control-plane-siem-checkpoint-v1" {
		return fmt.Errorf("unsupported secondary checkpoint version %q", checkpoint.Version)
	}
	requiredPrefix := uint64(0)
	if first > 1 {
		requiredPrefix = first - 1
	}
	if checkpoint.HighWatermarkSequence < requiredPrefix {
		return fmt.Errorf("secondary receiver is behind: checkpoint=%d required_prefix=%d", checkpoint.HighWatermarkSequence, requiredPrefix)
	}
	if checkpoint.HighWatermarkSequence == 0 {
		return nil
	}
	auditLookup, ok := w.repo.(interface {
		GetControlPlaneAuditBySequence(context.Context, uint64) (domain.ControlPlaneAudit, error)
	})
	if !ok {
		return fmt.Errorf("delivery repository does not support audit lookup required for receiver failover")
	}
	audit, err := auditLookup.GetControlPlaneAuditBySequence(ctx, checkpoint.HighWatermarkSequence)
	if err != nil {
		return fmt.Errorf("load local audit for secondary checkpoint sequence=%d: %w", checkpoint.HighWatermarkSequence, err)
	}
	if audit.RecordHash != strings.TrimSpace(checkpoint.HeadRecordHash) {
		return fmt.Errorf("secondary receiver checkpoint diverges at sequence=%d", checkpoint.HighWatermarkSequence)
	}
	return nil
}

func (w *DeliveryWorker) postEndpoint(ctx context.Context, endpoint, expectedReceiver string, receiverTerm uint64, body []byte, first, last uint64, idempotencyKey string) (domain.ControlPlaneSIEMAcknowledgement, []byte, error) {
	var result domain.ControlPlaneSIEMAcknowledgement
	signature, err := w.signer.Sign(body)
	if err != nil {
		return result, nil, fmt.Errorf("sign SIEM delivery: %w", err)
	}
	algorithm := "hmac-sha256"
	if provider, ok := w.signer.(SignatureAlgorithmProvider); ok {
		if value := strings.TrimSpace(provider.Algorithm()); value != "" {
			algorithm = value
		}
	}
	previousSignature := ""
	previousKeyID := ""
	previousAlgorithm := ""
	if provider, ok := w.signer.(PreviousSignatureProvider); ok {
		previousKeyID = strings.TrimSpace(provider.PreviousKeyID())
		if previousKeyID != "" {
			previousSignature, err = provider.SignPrevious(body)
			if err != nil {
				return result, nil, fmt.Errorf("sign SIEM delivery with previous key: %w", err)
			}
			previousAlgorithm = strings.TrimSpace(provider.PreviousAlgorithm())
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return result, nil, fmt.Errorf("build SIEM request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TaskFlow-Audit-Signature", signature)
	req.Header.Set("X-TaskFlow-Audit-Key-ID", w.signer.KeyID())
	req.Header.Set("X-TaskFlow-Audit-Signature-Algorithm", algorithm)
	if previousKeyID != "" && previousSignature != "" {
		req.Header.Set("X-TaskFlow-Audit-Previous-Signature", previousSignature)
		req.Header.Set("X-TaskFlow-Audit-Previous-Key-ID", previousKeyID)
		req.Header.Set("X-TaskFlow-Audit-Previous-Signature-Algorithm", previousAlgorithm)
	}
	req.Header.Set("X-TaskFlow-Audit-First-Sequence", strconv.FormatUint(first, 10))
	req.Header.Set("X-TaskFlow-Audit-Last-Sequence", strconv.FormatUint(last, 10))
	if receiverTerm > 0 {
		req.Header.Set("X-TaskFlow-Receiver-Term", strconv.FormatUint(receiverTerm, 10))
	}
	req.Header.Set("Idempotency-Key", idempotencyKey)
	if token := strings.TrimSpace(w.config.BearerToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return result, nil, fmt.Errorf("post SIEM delivery: %w", err)
	}
	defer resp.Body.Close()
	ackBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return result, nil, fmt.Errorf("read SIEM acknowledgement: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, nil, fmt.Errorf("SIEM delivery returned HTTP %d", resp.StatusCode)
	}
	var ack deliveryAcknowledgement
	if err := json.Unmarshal(ackBody, &ack); err != nil {
		appmetrics.ControlPlaneSIEMAcks.WithLabelValues("invalid").Inc()
		return result, nil, fmt.Errorf("decode SIEM acknowledgement: %w", err)
	}
	payloadDigest := sha256.Sum256(body)
	payloadSHA := hex.EncodeToString(payloadDigest[:])
	ack.Status = strings.ToLower(strings.TrimSpace(ack.Status))
	if ack.Status != "accepted" && ack.Status != "duplicate" {
		appmetrics.ControlPlaneSIEMAcks.WithLabelValues("invalid").Inc()
		return result, nil, fmt.Errorf("SIEM acknowledgement status %q is not accepted or duplicate", ack.Status)
	}
	if ack.IdempotencyKey != idempotencyKey || ack.FirstSequence != first || ack.LastSequence != last || !strings.EqualFold(ack.PayloadSHA256, payloadSHA) || strings.TrimSpace(ack.ReceiptID) == "" {
		appmetrics.ControlPlaneSIEMAcks.WithLabelValues("invalid").Inc()
		return result, nil, fmt.Errorf("SIEM acknowledgement does not match delivery batch")
	}
	if expected := strings.TrimSpace(expectedReceiver); expected != "" {
		if strings.TrimSpace(ack.ReceiverID) != expected || ack.ReceiverGeneration == 0 {
			appmetrics.ControlPlaneSIEMAcks.WithLabelValues("invalid_receiver").Inc()
			return result, nil, fmt.Errorf("SIEM acknowledgement receiver identity mismatch: got=%q generation=%d want=%q", ack.ReceiverID, ack.ReceiverGeneration, expected)
		}
		if receiverTerm > 0 && ack.ReceiverTerm != receiverTerm {
			appmetrics.ControlPlaneSIEMAcks.WithLabelValues("invalid_term").Inc()
			return result, nil, fmt.Errorf("SIEM acknowledgement receiver term mismatch: got=%d want=%d", ack.ReceiverTerm, receiverTerm)
		}
	}
	appmetrics.ControlPlaneSIEMAcks.WithLabelValues(ack.Status).Inc()
	ackDigest := sha256.Sum256(ackBody)
	result = domain.ControlPlaneSIEMAcknowledgement{
		ReceiptID:      strings.TrimSpace(ack.ReceiptID),
		IdempotencyKey: idempotencyKey,
		PayloadSHA256:  payloadSHA,
		AckHash:        hex.EncodeToString(ackDigest[:]),
		Status:         ack.Status,
		ReceiverID:     strings.TrimSpace(ack.ReceiverID),
		ReceiverGen:    ack.ReceiverGeneration,
		ReceiverTerm:   ack.ReceiverTerm,
	}
	receiptProof := deliveryReceiptProof{
		IdempotencyKey:     ack.IdempotencyKey,
		FirstSequence:      ack.FirstSequence,
		LastSequence:       ack.LastSequence,
		PayloadSHA256:      strings.ToLower(ack.PayloadSHA256),
		ReceiptID:          strings.TrimSpace(ack.ReceiptID),
		ReceiverID:         strings.TrimSpace(ack.ReceiverID),
		ReceiverGeneration: ack.ReceiverGeneration,
		ReceiverTerm:       ack.ReceiverTerm,
	}
	receiptProofBody, err := json.Marshal(receiptProof)
	if err != nil {
		return result, nil, fmt.Errorf("marshal SIEM receiver receipt proof: %w", err)
	}
	receiptProofDigest := sha256.Sum256(receiptProofBody)
	receiverReceipts := []deliveryReceiptProof{receiptProof}
	receiverReceiptsBody, err := json.Marshal(receiverReceipts)
	if err != nil {
		return result, nil, fmt.Errorf("marshal SIEM receiver receipt proof set: %w", err)
	}
	receiverReceiptsDigest := sha256.Sum256(receiverReceiptsBody)
	evidenceBody, err := json.Marshal(deliveryEvidence{
		Version:                "taskflow-control-plane-siem-evidence-v1",
		IdempotencyKey:         idempotencyKey,
		FirstSequence:          first,
		LastSequence:           last,
		PayloadSHA256:          payloadSHA,
		Envelope:               json.RawMessage(body),
		KeyID:                  w.signer.KeyID(),
		Algorithm:              algorithm,
		Signature:              signature,
		PreviousKeyID:          previousKeyID,
		PreviousAlgorithm:      previousAlgorithm,
		PreviousSignature:      previousSignature,
		ReceiverReceipt:        receiptProof,
		ReceiptProofSHA256:     hex.EncodeToString(receiptProofDigest[:]),
		ReceiverReceipts:       receiverReceipts,
		ReceiverReceiptsSHA256: hex.EncodeToString(receiverReceiptsDigest[:]),
	})
	if err != nil {
		return result, nil, fmt.Errorf("marshal SIEM delivery evidence: %w", err)
	}
	return result, evidenceBody, nil
}

func appendReceiverProofToEvidence(evidenceBody []byte, ack domain.ControlPlaneSIEMAcknowledgement, first, last uint64) ([]byte, error) {
	var evidence deliveryEvidence
	if err := json.Unmarshal(evidenceBody, &evidence); err != nil {
		return nil, fmt.Errorf("decode SIEM evidence before adding receiver proof: %w", err)
	}
	proof := deliveryReceiptProof{
		IdempotencyKey:     ack.IdempotencyKey,
		FirstSequence:      first,
		LastSequence:       last,
		PayloadSHA256:      strings.ToLower(ack.PayloadSHA256),
		ReceiptID:          strings.TrimSpace(ack.ReceiptID),
		ReceiverID:         strings.TrimSpace(ack.ReceiverID),
		ReceiverGeneration: ack.ReceiverGen,
		ReceiverTerm:       ack.ReceiverTerm,
	}
	proofFound := false
	for _, existing := range evidence.ReceiverReceipts {
		if existing == proof {
			proofFound = true
			break
		}
	}
	if !proofFound {
		evidence.ReceiverReceipts = append(evidence.ReceiverReceipts, proof)
	}
	sort.Slice(evidence.ReceiverReceipts, func(i, j int) bool {
		if evidence.ReceiverReceipts[i].ReceiverID != evidence.ReceiverReceipts[j].ReceiverID {
			return evidence.ReceiverReceipts[i].ReceiverID < evidence.ReceiverReceipts[j].ReceiverID
		}
		return evidence.ReceiverReceipts[i].ReceiverGeneration < evidence.ReceiverReceipts[j].ReceiverGeneration
	})
	proofSetBody, err := json.Marshal(evidence.ReceiverReceipts)
	if err != nil {
		return nil, fmt.Errorf("marshal SIEM receiver receipt proof set: %w", err)
	}
	proofSetDigest := sha256.Sum256(proofSetBody)
	evidence.ReceiverReceiptsSHA256 = hex.EncodeToString(proofSetDigest[:])
	return json.Marshal(evidence)
}

func deliveryEvidenceObjectKey(first, last uint64, idempotencyKey string) string {
	return fmt.Sprintf("control-plane-audit/%020d-%020d-%s.json", first, last, idempotencyKey)
}

func (w *DeliveryWorker) refreshStats(ctx context.Context) {
	stats, err := w.repo.ControlPlaneAuditDeliveryStats(ctx)
	if err != nil {
		return
	}
	appmetrics.ControlPlaneSIEMDeliveryBacklog.Set(float64(stats.Pending + stats.Delivering))
	appmetrics.ControlPlaneSIEMDeliveryDLQ.Set(float64(stats.DLQ))
}

func retryDelay(base, max time.Duration, attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := base
	for i := 1; i < attempts && delay < max; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func maxDeliveryAttempts(deliveries []domain.ControlPlaneAuditDelivery) int {
	max := 1
	for _, delivery := range deliveries {
		if delivery.Attempts > max {
			max = delivery.Attempts
		}
	}
	return max
}

func randomLeaseToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func deliveryIdempotencyKey(deliveries []domain.ControlPlaneAuditDelivery) string {
	h := sha256.New()
	for _, delivery := range deliveries {
		_, _ = fmt.Fprintf(h, "%d:%s\n", delivery.ChainSequence, delivery.RecordHash)
	}
	sum := h.Sum(nil)
	return "taskflow-audit-" + hex.EncodeToString(sum[:16])
}
