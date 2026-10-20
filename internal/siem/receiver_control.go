package siem

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type receiverRoutingRepository interface {
	GetControlPlaneSIEMReceiverRoutingState(context.Context, string) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	FindControlPlaneSIEMReceiverRoutingState(context.Context) (domain.ControlPlaneSIEMReceiverRoutingState, bool, error)
	RestoreControlPlaneSIEMReceiverRoutingState(context.Context, string, uint64, string, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	PromoteControlPlaneSIEMReceiver(context.Context, string, string, uint64, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	GetControlPlaneAuditBySequence(context.Context, uint64) (domain.ControlPlaneAudit, error)
}

type receiverTermFenceRequest struct {
	Term           uint64 `json:"term"`
	ActiveReceiver string `json:"active_receiver"`
}

type receiverTermFenceResponse struct {
	ReceiverID string `json:"receiver_id"`
	Term       uint64 `json:"term"`
}

func (w *DeliveryWorker) EnsureReceiverRoutingState(ctx context.Context) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	repo, err := w.routingRepository()
	if err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	state, found, err := repo.FindControlPlaneSIEMReceiverRoutingState(ctx)
	if err != nil {
		return state, err
	}
	if found {
		return repo.GetControlPlaneSIEMReceiverRoutingState(ctx, w.config.PrimaryReceiver)
	}
	if w.config.CheckpointVerifier == nil {
		return state, fmt.Errorf("cannot restore SIEM receiver routing state without signed checkpoints")
	}
	primary, err := w.fetchReceiverHead(ctx, w.primaryCheckpointEndpoint(), w.config.PrimaryReceiver)
	if err != nil {
		return state, fmt.Errorf("restore routing state from primary checkpoint: %w", err)
	}
	secondary, err := w.fetchReceiverHead(ctx, w.config.SecondaryCheckpointEndpoint, w.config.SecondaryReceiver)
	if err != nil {
		return state, fmt.Errorf("restore routing state from secondary checkpoint: %w", err)
	}
	if primary.ReceiverTerm == 0 || primary.ReceiverTerm != secondary.ReceiverTerm {
		return state, fmt.Errorf("signed receiver checkpoints disagree on routing term: primary=%d secondary=%d", primary.ReceiverTerm, secondary.ReceiverTerm)
	}
	primaryActive := strings.TrimSpace(primary.ActiveReceiver)
	secondaryActive := strings.TrimSpace(secondary.ActiveReceiver)
	if primaryActive == "" || primaryActive != secondaryActive {
		return state, fmt.Errorf("signed receiver checkpoints disagree on active receiver: primary=%q secondary=%q", primaryActive, secondaryActive)
	}
	if primaryActive != w.config.PrimaryReceiver && primaryActive != w.config.SecondaryReceiver {
		return state, fmt.Errorf("signed receiver checkpoints name unknown active receiver %q", primaryActive)
	}
	restored, err := repo.RestoreControlPlaneSIEMReceiverRoutingState(ctx, primaryActive, primary.ReceiverTerm, "receiver-checkpoint-dr", time.Now())
	if err != nil {
		return restored, err
	}
	appmetrics.ControlPlaneSIEMRoutingRecovery.Inc()
	appmetrics.ControlPlaneSIEMReceiverTerm.Set(float64(restored.Term))
	return restored, nil
}

func (w *DeliveryWorker) ReceiverStatus(ctx context.Context) (domain.ControlPlaneSIEMReceiverStatus, error) {
	var status domain.ControlPlaneSIEMReceiverStatus
	if strings.TrimSpace(w.config.PrimaryReceiver) == "" || strings.TrimSpace(w.config.SecondaryReceiver) == "" {
		return status, fmt.Errorf("SIEM receiver cluster is not configured")
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return status, fmt.Errorf("load SIEM receiver routing state: %w", err)
	}
	primary, err := w.fetchReceiverHead(ctx, w.primaryCheckpointEndpoint(), w.config.PrimaryReceiver)
	if err != nil {
		return status, fmt.Errorf("load primary receiver status: %w", err)
	}
	secondary, err := w.fetchReceiverHead(ctx, w.config.SecondaryCheckpointEndpoint, w.config.SecondaryReceiver)
	if err != nil {
		return status, fmt.Errorf("load secondary receiver status: %w", err)
	}
	rpo := sequenceDistance(primary.HighWatermarkSequence, secondary.HighWatermarkSequence)
	if w.config.RPOPolicyEnabled {
		state, err = w.observeRPOPolicy(ctx, rpo)
		if err != nil {
			return status, fmt.Errorf("observe SIEM receiver RPO policy: %w", err)
		}
	}
	status = domain.ControlPlaneSIEMReceiverStatus{
		ActiveReceiver:              state.ActiveReceiver,
		Term:                        state.Term,
		PrimaryHighWatermark:        primary.HighWatermarkSequence,
		SecondaryHighWatermark:      secondary.HighWatermarkSequence,
		PrimaryReceiverGeneration:   primary.ReceiverGeneration,
		SecondaryReceiverGeneration: secondary.ReceiverGeneration,
		PrimaryReceiverTerm:         primary.ReceiverTerm,
		SecondaryReceiverTerm:       secondary.ReceiverTerm,
		RPOSequences:                rpo,
		RPOFenced:                   state.RPOFenced,
		RPOBreachSince:              state.RPOBreachSince,
		RPOFenceReason:              state.RPOFenceReason,
	}
	appmetrics.ControlPlaneSIEMReceiverRPO.Set(float64(status.RPOSequences))
	appmetrics.ControlPlaneSIEMReceiverTerm.Set(float64(status.Term))
	return status, nil
}

func (w *DeliveryWorker) PromoteReceiver(ctx context.Context, target string, expectedTerm uint64, operator string) (domain.ControlPlaneSIEMReceiverPromotionResult, error) {
	var result domain.ControlPlaneSIEMReceiverPromotionResult
	started := time.Now()
	target = strings.TrimSpace(target)
	operator = strings.TrimSpace(operator)
	if !w.config.TermFencingEnabled {
		return result, fmt.Errorf("SIEM receiver term fencing is disabled")
	}
	if w.config.PromotionWorkflowEnabled {
		return result, fmt.Errorf("SIEM receiver promotion workflow requires an approved promotion intent")
	}
	if target != w.config.PrimaryReceiver && target != w.config.SecondaryReceiver {
		return result, fmt.Errorf("unknown SIEM receiver %q", target)
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return result, err
	}
	if expectedTerm == 0 || expectedTerm != state.Term {
		return result, fmt.Errorf("SIEM receiver promotion expected_term=%d current_term=%d", expectedTerm, state.Term)
	}
	if state.ActiveReceiver == target {
		return domain.ControlPlaneSIEMReceiverPromotionResult{
			ActiveReceiver: state.ActiveReceiver,
			Term:           state.Term,
			PreviousTerm:   state.Term,
			PromotedBy:     state.PromotedBy,
		}, nil
	}
	status, err := w.ReceiverStatus(ctx)
	if err != nil {
		return result, err
	}
	if status.RPOFenced {
		return result, fmt.Errorf("receiver promotion fenced by RPO policy: %s", status.RPOFenceReason)
	}
	if status.PrimaryHighWatermark != status.SecondaryHighWatermark {
		return result, fmt.Errorf("receiver promotion target is not caught up: primary=%d secondary=%d", status.PrimaryHighWatermark, status.SecondaryHighWatermark)
	}
	newState, err := w.promoteReceiverTerm(ctx, target, state.Term, operator)
	if err != nil {
		appmetrics.ControlPlaneSIEMSwitchover.WithLabelValues(receiverRole(w, target), "failed").Inc()
		return result, err
	}
	appmetrics.ControlPlaneSIEMSwitchover.WithLabelValues(receiverRole(w, target), "success").Inc()
	appmetrics.ControlPlaneSIEMSwitchoverRTO.Set(time.Since(started).Seconds())
	return domain.ControlPlaneSIEMReceiverPromotionResult{
		ActiveReceiver: newState.ActiveReceiver,
		Term:           newState.Term,
		PreviousTerm:   state.Term,
		PromotedBy:     newState.PromotedBy,
	}, nil
}

func (w *DeliveryWorker) SyncReceiverAntiEntropy(ctx context.Context) (int, error) {
	if !w.config.AntiEntropyEnabled {
		return 0, nil
	}
	if strings.TrimSpace(w.config.PrimaryReceiver) == "" || strings.TrimSpace(w.config.SecondaryReceiver) == "" || w.config.CheckpointVerifier == nil {
		return 0, fmt.Errorf("SIEM anti-entropy requires two signed-checkpoint receivers")
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return 0, err
	}
	primary, err := w.fetchReceiverHead(ctx, w.primaryCheckpointEndpoint(), w.config.PrimaryReceiver)
	if err != nil {
		return 0, fmt.Errorf("fetch primary receiver checkpoint for anti-entropy: %w", err)
	}
	secondary, err := w.fetchReceiverHead(ctx, w.config.SecondaryCheckpointEndpoint, w.config.SecondaryReceiver)
	if err != nil {
		return 0, fmt.Errorf("fetch secondary receiver checkpoint for anti-entropy: %w", err)
	}
	rpo := sequenceDistance(primary.HighWatermarkSequence, secondary.HighWatermarkSequence)
	appmetrics.ControlPlaneSIEMReceiverRPO.Set(float64(rpo))
	if w.config.RPOPolicyEnabled {
		state, err = w.observeRPOPolicy(ctx, rpo)
		if err != nil {
			return 0, fmt.Errorf("observe SIEM receiver RPO policy: %w", err)
		}
	}
	if rpo == 0 {
		return 0, nil
	}

	var source, target receiverCheckpointResponse
	var targetEndpoint, targetReceiver string
	if state.ActiveReceiver == w.config.PrimaryReceiver {
		if secondary.HighWatermarkSequence > primary.HighWatermarkSequence {
			return 0, fmt.Errorf("standby secondary is ahead of authoritative primary; operator promotion is required")
		}
		source, target = primary, secondary
		targetEndpoint, targetReceiver = w.config.SecondaryEndpoint, w.config.SecondaryReceiver
	} else if state.ActiveReceiver == w.config.SecondaryReceiver {
		if primary.HighWatermarkSequence > secondary.HighWatermarkSequence {
			return 0, fmt.Errorf("standby primary is ahead of authoritative secondary; operator promotion is required")
		}
		source, target = secondary, primary
		targetEndpoint, targetReceiver = w.config.Endpoint, w.config.PrimaryReceiver
	} else {
		return 0, fmt.Errorf("routing state contains unknown active receiver %q", state.ActiveReceiver)
	}

	replayed := 0
	for sequence := target.HighWatermarkSequence + 1; sequence <= source.HighWatermarkSequence; sequence++ {
		audit, err := w.getAuditBySequence(ctx, sequence)
		if err != nil {
			return replayed, fmt.Errorf("load audit sequence=%d for anti-entropy: %w", sequence, err)
		}
		body, idempotencyKey, _, err := canonicalDeliveryIdentityFromAudit(audit)
		if err != nil {
			return replayed, err
		}
		ack, _, err := w.postEndpoint(ctx, targetEndpoint, targetReceiver, state.Term, body, sequence, sequence, idempotencyKey)
		if err != nil {
			return replayed, fmt.Errorf("anti-entropy receiver=%s sequence=%d: %w", targetReceiver, sequence, err)
		}
		if writer, ok := w.config.EvidenceReplica.(ReceiverReceiptEvidenceWriter); ok {
			proof := deliveryReceiptProof{
				IdempotencyKey:     ack.IdempotencyKey,
				FirstSequence:      sequence,
				LastSequence:       sequence,
				PayloadSHA256:      ack.PayloadSHA256,
				ReceiptID:          ack.ReceiptID,
				ReceiverID:         ack.ReceiverID,
				ReceiverGeneration: ack.ReceiverGen,
				ReceiverTerm:       ack.ReceiverTerm,
			}
			if _, err := writer.ReplicateReceiverReceipt(ctx, sequence, idempotencyKey, proof); err != nil {
				return replayed, fmt.Errorf("persist anti-entropy receiver proof sequence=%d: %w", sequence, err)
			}
		}
		replayed++
		appmetrics.ControlPlaneSIEMAntiEntropyReplayed.Inc()
	}
	verified, err := w.fetchReceiverHead(ctx, checkpointEndpointForReceiver(w, targetReceiver), targetReceiver)
	if err != nil {
		return replayed, fmt.Errorf("verify anti-entropy target checkpoint: %w", err)
	}
	if verified.HighWatermarkSequence != source.HighWatermarkSequence || verified.HeadRecordHash != source.HeadRecordHash {
		return replayed, fmt.Errorf("anti-entropy target did not converge: target=%d source=%d", verified.HighWatermarkSequence, source.HighWatermarkSequence)
	}
	if w.config.RPOPolicyEnabled {
		if _, err := w.observeRPOPolicy(ctx, 0); err != nil {
			return replayed, fmt.Errorf("clear SIEM receiver RPO policy fence: %w", err)
		}
	}
	appmetrics.ControlPlaneSIEMReceiverRPO.Set(0)
	return replayed, nil
}

func (w *DeliveryWorker) promoteReceiverTerm(ctx context.Context, target string, expectedTerm uint64, operator string) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	if w.config.PromotionWorkflowEnabled {
		action := "coordinated-direct"
		if operator == "automatic-failover" {
			action = "automatic-failover"
		}
		return w.promoteReceiverTermCoordinated(ctx, target, expectedTerm, 0, operator, operator, "", "", action, 0, false, "")
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return state, err
	}
	if state.Term != expectedTerm {
		return state, fmt.Errorf("SIEM receiver term changed: got=%d expected=%d", state.Term, expectedTerm)
	}
	if state.ActiveReceiver == target {
		return state, nil
	}
	candidate := state.Term + 1
	if err := w.fenceReceiverTerm(ctx, w.config.PrimaryTermEndpoint, w.config.PrimaryReceiver, target, candidate); err != nil {
		return state, fmt.Errorf("fence primary receiver at term=%d: %w", candidate, err)
	}
	if err := w.fenceReceiverTerm(ctx, w.config.SecondaryTermEndpoint, w.config.SecondaryReceiver, target, candidate); err != nil {
		return state, fmt.Errorf("fence secondary receiver at term=%d: %w", candidate, err)
	}
	repo, err := w.routingRepository()
	if err != nil {
		return state, err
	}
	promoted, err := repo.PromoteControlPlaneSIEMReceiver(ctx, target, operator, expectedTerm, time.Now())
	if err != nil {
		return promoted, err
	}
	if promoted.Term != candidate || promoted.ActiveReceiver != target {
		return promoted, fmt.Errorf("persisted SIEM receiver promotion mismatch: receiver=%s term=%d", promoted.ActiveReceiver, promoted.Term)
	}
	appmetrics.ControlPlaneSIEMReceiverTerm.Set(float64(promoted.Term))
	return promoted, nil
}

func (w *DeliveryWorker) fenceReceiverTerm(ctx context.Context, endpoint, expectedReceiver, activeReceiver string, term uint64) error {
	body, err := json.Marshal(receiverTermFenceRequest{Term: term, ActiveReceiver: activeReceiver})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(w.config.BearerToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("receiver term fence returned HTTP %d", resp.StatusCode)
	}
	var result receiverTermFenceResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return fmt.Errorf("decode receiver term fence response: %w", err)
	}
	if result.ReceiverID != expectedReceiver || result.Term != term {
		return fmt.Errorf("receiver term fence mismatch receiver=%q term=%d", result.ReceiverID, result.Term)
	}
	return nil
}

func (w *DeliveryWorker) fetchReceiverHead(ctx context.Context, endpoint, receiverID string) (receiverCheckpointResponse, error) {
	checkpoint, err := fetchReceiverCheckpoint(ctx, w.client, endpoint, w.config.BearerToken, 0, 1, w.config.CheckpointVerifier, receiverID)
	if err != nil {
		return checkpoint, err
	}
	if checkpoint.Version != "taskflow-control-plane-siem-checkpoint-v1" {
		return checkpoint, fmt.Errorf("unsupported receiver checkpoint version %q", checkpoint.Version)
	}
	if checkpoint.HighWatermarkSequence > 0 {
		audit, err := w.getAuditBySequence(ctx, checkpoint.HighWatermarkSequence)
		if err != nil {
			return checkpoint, err
		}
		if audit.RecordHash != strings.TrimSpace(checkpoint.HeadRecordHash) {
			return checkpoint, fmt.Errorf("receiver checkpoint head diverges at sequence=%d", checkpoint.HighWatermarkSequence)
		}
	}
	return checkpoint, nil
}

func checkpointEndpointForEvents(eventsEndpoint, termEndpoint string) string {
	if strings.TrimSpace(termEndpoint) != "" && strings.HasSuffix(strings.TrimSpace(termEndpoint), "/control/term") {
		return strings.TrimSuffix(strings.TrimSpace(termEndpoint), "/control/term") + "/checkpoint"
	}
	return strings.TrimSuffix(strings.TrimSpace(eventsEndpoint), "/events") + "/checkpoint"
}

func checkpointEndpointForReceiver(w *DeliveryWorker, receiverID string) string {
	if receiverID == w.config.SecondaryReceiver {
		return w.config.SecondaryCheckpointEndpoint
	}
	return w.primaryCheckpointEndpoint()
}

func (w *DeliveryWorker) primaryCheckpointEndpoint() string {
	if endpoint := strings.TrimSpace(w.config.PrimaryCheckpointEndpoint); endpoint != "" {
		return endpoint
	}
	return checkpointEndpointForEvents(w.config.Endpoint, w.config.PrimaryTermEndpoint)
}

func sequenceDistance(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

func receiverRole(w *DeliveryWorker, receiverID string) string {
	if receiverID == w.config.SecondaryReceiver {
		return "secondary"
	}
	return "primary"
}

func (w *DeliveryWorker) routingRepository() (receiverRoutingRepository, error) {
	repo, ok := w.repo.(receiverRoutingRepository)
	if !ok {
		return nil, fmt.Errorf("delivery repository does not support Phase 30 receiver routing")
	}
	return repo, nil
}

func (w *DeliveryWorker) getReceiverRoutingState(ctx context.Context) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	repo, err := w.routingRepository()
	if err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	return repo.GetControlPlaneSIEMReceiverRoutingState(ctx, w.config.PrimaryReceiver)
}

func (w *DeliveryWorker) getAuditBySequence(ctx context.Context, sequence uint64) (domain.ControlPlaneAudit, error) {
	repo, err := w.routingRepository()
	if err != nil {
		return domain.ControlPlaneAudit{}, err
	}
	return repo.GetControlPlaneAuditBySequence(ctx, sequence)
}
