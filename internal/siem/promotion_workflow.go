package siem

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

type receiverPromotionRepository interface {
	CreateControlPlaneSIEMPromotionIntent(context.Context, string, uint64, string, string, int, time.Time, time.Time) (domain.ControlPlaneSIEMPromotionIntent, error)
	ApproveControlPlaneSIEMPromotionIntent(context.Context, uint64, string, string, time.Time) (domain.ControlPlaneSIEMPromotionIntent, error)
	ApproveControlPlaneSIEMPromotionIntentWithQuorum(context.Context, uint64, string, string, string, int, time.Time) (domain.ControlPlaneSIEMPromotionIntent, error)
	ListControlPlaneSIEMPromotionApprovals(context.Context, uint64) ([]domain.ControlPlaneSIEMPromotionApproval, error)
	CancelActiveControlPlaneSIEMPromotionIntents(context.Context, uint64, time.Time) (int64, error)
	GetControlPlaneSIEMPromotionIntent(context.Context, uint64) (domain.ControlPlaneSIEMPromotionIntent, error)
	ListControlPlaneSIEMPromotionIntents(context.Context, int) ([]domain.ControlPlaneSIEMPromotionIntent, error)
	ClaimControlPlaneSIEMPromotionExecution(context.Context, uint64, string, uint64, string, time.Time, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	ReleaseControlPlaneSIEMPromotionExecution(context.Context, uint64, string, time.Time) error
	CommitControlPlaneSIEMPromotionExecution(context.Context, string, string, string, string, string, string, uint64, uint64, uint64, string, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	CommitControlPlaneSIEMPromotionExecutionV2(context.Context, string, string, string, string, string, string, uint64, uint64, uint64, string, bool, string, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	ObserveControlPlaneSIEMReceiverRPO(context.Context, uint64, uint64, time.Duration, time.Time) (domain.ControlPlaneSIEMReceiverRoutingState, error)
	ListControlPlaneSIEMReceiverTermHistory(context.Context, int) ([]domain.ControlPlaneSIEMReceiverTermHistory, error)
	GetControlPlaneSIEMReceiverTermHistoryByTerm(context.Context, uint64) (domain.ControlPlaneSIEMReceiverTermHistory, error)
	RecordControlPlaneSIEMTermLedgerAnchor(context.Context, domain.ControlPlaneSIEMTermLedgerAnchor) error
	GetControlPlaneSIEMTermLedgerAnchor(context.Context, uint64) (domain.ControlPlaneSIEMTermLedgerAnchor, bool, error)
	RecordControlPlaneSIEMGovernanceReplicaReceipts(context.Context, []domain.ControlPlaneSIEMGovernanceReplicaReceipt) error
	ListControlPlaneSIEMGovernanceReplicaReceipts(context.Context, string, uint64, int) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error)
	ClaimControlPlaneSIEMTermAnchorDeliveries(context.Context, time.Time, time.Time, string, int) ([]domain.ControlPlaneSIEMReceiverTermHistory, error)
	MarkControlPlaneSIEMTermAnchorDelivered(context.Context, uint64, time.Time) error
	FailControlPlaneSIEMTermAnchorDelivery(context.Context, domain.ControlPlaneSIEMReceiverTermHistory, string, time.Time, time.Time, error, int) (bool, error)
	CreateControlPlaneSIEMIncidentEvidence(context.Context, domain.ControlPlaneSIEMIncidentEvidence) (domain.ControlPlaneSIEMIncidentEvidence, error)
	ListControlPlaneSIEMIncidentEvidence(context.Context, int) ([]domain.ControlPlaneSIEMIncidentEvidence, error)
	ClaimControlPlaneSIEMIncidentAlertDeliveries(context.Context, time.Time, time.Time, string, int) ([]domain.ControlPlaneSIEMIncidentEvidence, error)
	MarkControlPlaneSIEMIncidentAlertDelivered(context.Context, uint64, time.Time) error
	FailControlPlaneSIEMIncidentAlertDelivery(context.Context, domain.ControlPlaneSIEMIncidentEvidence, string, time.Time, time.Time, error, int) (bool, error)
	ControlPlaneSIEMGovernanceDeliveryStatus(context.Context, time.Time) (domain.ControlPlaneSIEMGovernanceStatus, error)
	ListControlPlaneSIEMGovernanceDead(context.Context, string, int) ([]domain.ControlPlaneSIEMGovernanceDeadItem, error)
	RequeueControlPlaneSIEMGovernanceDead(context.Context, string, uint64, string, string, time.Time) (domain.ControlPlaneSIEMGovernanceRemediation, error)
	RequestControlPlaneSIEMGovernanceRemediation(context.Context, string, uint64, string, string, string, time.Time, time.Time) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error)
	ApproveControlPlaneSIEMGovernanceRemediation(context.Context, uint64, string, string, bool, time.Time) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error)
	RejectControlPlaneSIEMGovernanceRemediation(context.Context, uint64, string, string, time.Time) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error)
	CancelControlPlaneSIEMGovernanceRemediation(context.Context, uint64, string, string, time.Time) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error)
	ExpireControlPlaneSIEMGovernanceRemediations(context.Context, time.Time) (int64, error)
	ListControlPlaneSIEMGovernanceRemediations(context.Context, int) ([]domain.ControlPlaneSIEMGovernanceRemediation, error)
	SettleControlPlaneSIEMGovernanceRemediations(context.Context, string, uint64, string, string, time.Time) (bool, error)
}

func (w *DeliveryWorker) RequestPromotionIntent(
	ctx context.Context,
	target string,
	expectedTerm uint64,
	reason, operator string,
) (domain.ControlPlaneSIEMPromotionIntent, error) {
	if !w.config.PromotionWorkflowEnabled {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("SIEM receiver promotion workflow is disabled")
	}
	target = strings.TrimSpace(target)
	operator = strings.TrimSpace(operator)
	reason = strings.TrimSpace(reason)
	if target != w.config.PrimaryReceiver && target != w.config.SecondaryReceiver {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("unknown SIEM receiver %q", target)
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return domain.ControlPlaneSIEMPromotionIntent{}, err
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if w.config.PromotionRolePolicy != "" && !operatorHasRole(w.config.PromotionRolePolicy, operator, "requester") {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("operator %s lacks requester role", operator)
	}
	intent, err := repo.CreateControlPlaneSIEMPromotionIntent(ctx, target, expectedTerm, operator, reason, w.config.PromotionApprovalQuorum, now.Add(w.config.PromotionIntentTTL), now)
	if err != nil {
		appmetrics.ControlPlaneSIEMPromotionIntents.WithLabelValues("request_failed").Inc()
		return intent, err
	}
	appmetrics.ControlPlaneSIEMPromotionIntents.WithLabelValues("requested").Inc()
	return intent, nil
}

func (w *DeliveryWorker) ApprovePromotionIntent(
	ctx context.Context,
	id uint64,
	note, operator string,
) (domain.ControlPlaneSIEMPromotionIntent, error) {
	if !w.config.PromotionWorkflowEnabled {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("SIEM receiver promotion workflow is disabled")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return domain.ControlPlaneSIEMPromotionIntent{}, err
	}
	role := "approver"
	if w.config.PromotionRolePolicy != "" && !operatorHasRole(w.config.PromotionRolePolicy, operator, role) {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("operator %s lacks approver role", operator)
	}
	intent, err := repo.ApproveControlPlaneSIEMPromotionIntentWithQuorum(ctx, id, operator, role, note, w.config.PromotionApprovalQuorum, time.Now())
	if err != nil {
		appmetrics.ControlPlaneSIEMPromotionIntents.WithLabelValues("approval_failed").Inc()
		return intent, err
	}
	appmetrics.ControlPlaneSIEMPromotionIntents.WithLabelValues("approved").Inc()
	return intent, nil
}

func (w *DeliveryWorker) ExecutePromotionIntent(ctx context.Context, id uint64, operator string) (domain.ControlPlaneSIEMReceiverPromotionResult, error) {
	var result domain.ControlPlaneSIEMReceiverPromotionResult
	if !w.config.PromotionWorkflowEnabled {
		return result, fmt.Errorf("SIEM receiver promotion workflow is disabled")
	}
	if w.config.PromotionRolePolicy != "" && !operatorHasRole(w.config.PromotionRolePolicy, operator, "approver") {
		return result, fmt.Errorf("operator %s lacks approver role for promotion execution", operator)
	}
	governanceStatus, err := w.GovernanceStatus(ctx)
	if err != nil {
		return result, fmt.Errorf("evaluate governance debt before promotion: %w", err)
	}
	if governanceStatus.Fenced {
		appmetrics.ControlPlaneSIEMPromotionExecution.WithLabelValues("governance_fenced").Inc()
		return result, fmt.Errorf("receiver promotion fenced by governance debt policy: %s", governanceStatus.FenceReason)
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return result, err
	}
	intent, err := repo.GetControlPlaneSIEMPromotionIntent(ctx, id)
	if err != nil {
		return result, err
	}
	status, err := w.ReceiverStatus(ctx)
	if err != nil {
		return result, err
	}
	if status.RPOFenced {
		appmetrics.ControlPlaneSIEMPromotionExecution.WithLabelValues("rpo_fenced").Inc()
		return result, fmt.Errorf("receiver promotion fenced by RPO policy: %s", status.RPOFenceReason)
	}
	if status.PrimaryHighWatermark != status.SecondaryHighWatermark {
		return result, fmt.Errorf("receiver promotion target is not caught up: primary=%d secondary=%d", status.PrimaryHighWatermark, status.SecondaryHighWatermark)
	}
	started := time.Now()
	state, err := w.promoteReceiverTermCoordinated(
		ctx,
		intent.TargetReceiver,
		intent.ExpectedTerm,
		intent.ID,
		operator,
		intent.RequestedBy,
		intent.ApprovedBy,
		intent.Reason,
		"operator-approved",
		status.RPOSequences,
		false,
		"",
	)
	if err != nil {
		appmetrics.ControlPlaneSIEMPromotionExecution.WithLabelValues("failed").Inc()
		return result, err
	}
	appmetrics.ControlPlaneSIEMPromotionExecution.WithLabelValues("executed").Inc()
	appmetrics.ControlPlaneSIEMSwitchover.WithLabelValues(receiverRole(w, state.ActiveReceiver), "approved").Inc()
	rto := time.Since(started)
	appmetrics.ControlPlaneSIEMSwitchoverRTO.Set(rto.Seconds())
	result = domain.ControlPlaneSIEMReceiverPromotionResult{
		ActiveReceiver: state.ActiveReceiver,
		Term:           state.Term,
		PreviousTerm:   intent.ExpectedTerm,
		PromotedBy:     state.PromotedBy,
	}
	if w.config.Governance != nil {
		anchor, anchorErr := w.anchorTerm(ctx, state.Term)
		if anchorErr != nil {
			_ = w.recordIncident(ctx, "term-anchor-failure-"+fmt.Sprint(state.Term), "term_anchor_failure", "critical", state.Term, state.ActiveReceiver, status.RPOSequences, rto.Seconds(), anchorErr.Error())
		} else {
			result.Anchored = true
			result.AnchorID = anchor.AnchorID
		}
	}
	if w.config.RTOAlertThreshold > 0 && rto > w.config.RTOAlertThreshold {
		_ = w.recordIncident(ctx, "rto-term-"+fmt.Sprint(state.Term), "rto_breach", "warning", state.Term, state.ActiveReceiver, status.RPOSequences, rto.Seconds(), fmt.Sprintf("switchover RTO %s exceeds threshold %s", rto, w.config.RTOAlertThreshold))
	}
	return result, nil
}

func (w *DeliveryWorker) ListPromotionIntents(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMPromotionIntent, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMPromotionIntents(ctx, limit)
}

func (w *DeliveryWorker) PromotionApprovals(ctx context.Context, intentID uint64) ([]domain.ControlPlaneSIEMPromotionApproval, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMPromotionApprovals(ctx, intentID)
}

func (w *DeliveryWorker) BreakGlassPromotion(ctx context.Context, target string, expectedTerm uint64, incidentID, reason, operator string) (domain.ControlPlaneSIEMReceiverPromotionResult, error) {
	var result domain.ControlPlaneSIEMReceiverPromotionResult
	if !w.config.BreakGlassEnabled {
		return result, fmt.Errorf("SIEM break-glass promotion is disabled")
	}
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "incident-commander") {
		return result, fmt.Errorf("operator %s lacks incident-commander role", operator)
	}
	if governanceStatus, statusErr := w.GovernanceStatus(ctx); statusErr == nil && governanceStatus.Fenced {
		appmetrics.ControlPlaneSIEMBreakGlass.WithLabelValues("governance_debt_bypass").Inc()
	}
	target = strings.TrimSpace(target)
	incidentID = strings.TrimSpace(incidentID)
	reason = strings.TrimSpace(reason)
	if incidentID == "" || reason == "" {
		return result, fmt.Errorf("break-glass incident_id and reason are required")
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return result, err
	}
	if state.Term != expectedTerm {
		return result, fmt.Errorf("break-glass expected_term=%d current_term=%d", expectedTerm, state.Term)
	}
	if target != w.config.PrimaryReceiver && target != w.config.SecondaryReceiver {
		return result, fmt.Errorf("unknown SIEM receiver %q", target)
	}
	if target == state.ActiveReceiver {
		return result, fmt.Errorf("receiver %s is already active", target)
	}
	started := time.Now()
	if _, err := w.SyncReceiverAntiEntropy(ctx); err != nil {
		return result, fmt.Errorf("break-glass target catch-up failed: %w", err)
	}
	status, err := w.ReceiverStatus(ctx)
	if err != nil {
		return result, err
	}
	if status.RPOSequences != 0 || status.PrimaryHighWatermark != status.SecondaryHighWatermark {
		return result, fmt.Errorf("break-glass refuses non-converged receivers: primary=%d secondary=%d", status.PrimaryHighWatermark, status.SecondaryHighWatermark)
	}
	if w.config.RPOPolicyEnabled {
		if _, err := w.observeRPOPolicy(ctx, 0); err != nil {
			return result, err
		}
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return result, err
	}
	if _, err := repo.CancelActiveControlPlaneSIEMPromotionIntents(ctx, expectedTerm, time.Now()); err != nil {
		return result, fmt.Errorf("cancel superseded promotion intents before break-glass: %w", err)
	}
	promoted, err := w.promoteReceiverTermCoordinated(ctx, target, expectedTerm, 0, operator, operator, "", reason, "break-glass", 0, true, incidentID)
	if err != nil {
		return result, err
	}
	rto := time.Since(started)
	appmetrics.ControlPlaneSIEMSwitchoverRTO.Set(rto.Seconds())
	appmetrics.ControlPlaneSIEMSwitchover.WithLabelValues(receiverRole(w, target), "break_glass").Inc()
	appmetrics.ControlPlaneSIEMBreakGlass.WithLabelValues("executed").Inc()
	result = domain.ControlPlaneSIEMReceiverPromotionResult{
		ActiveReceiver: promoted.ActiveReceiver,
		Term:           promoted.Term,
		PreviousTerm:   expectedTerm,
		PromotedBy:     promoted.PromotedBy,
		Emergency:      true,
		IncidentID:     incidentID,
	}
	if w.config.Governance != nil {
		anchor, anchorErr := w.anchorTerm(ctx, promoted.Term)
		if anchorErr != nil {
			_ = w.recordIncident(ctx, incidentID+"-anchor", "term_anchor_failure", "critical", promoted.Term, promoted.ActiveReceiver, 0, rto.Seconds(), anchorErr.Error())
		} else {
			result.Anchored = true
			result.AnchorID = anchor.AnchorID
		}
	}
	_ = w.recordIncident(ctx, incidentID, "break_glass", "critical", promoted.Term, promoted.ActiveReceiver, 0, rto.Seconds(), reason)
	if w.config.RTOAlertThreshold > 0 && rto > w.config.RTOAlertThreshold {
		_ = w.recordIncident(ctx, incidentID+"-rto", "rto_breach", "warning", promoted.Term, promoted.ActiveReceiver, 0, rto.Seconds(), fmt.Sprintf("break-glass RTO %s exceeds threshold %s", rto, w.config.RTOAlertThreshold))
	}
	return result, nil
}

func (w *DeliveryWorker) EnsureTermAnchor(ctx context.Context) (domain.ControlPlaneSIEMTermLedgerAnchor, error) {
	if w.config.Governance == nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, nil
	}
	state, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	return w.anchorTerm(ctx, state.Term)
}

func (w *DeliveryWorker) IncidentEvidence(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMIncidentEvidence, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMIncidentEvidence(ctx, limit)
}

func (w *DeliveryWorker) anchorTerm(ctx context.Context, term uint64) (domain.ControlPlaneSIEMTermLedgerAnchor, error) {
	if w.config.Governance == nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("SIEM governance anchoring is disabled")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	entry, err := repo.GetControlPlaneSIEMReceiverTermHistoryByTerm(ctx, term)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	computed, err := domain.ComputeControlPlaneSIEMReceiverTermHistoryHash(entry)
	if err != nil {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, err
	}
	if computed != entry.RecordHash {
		return domain.ControlPlaneSIEMTermLedgerAnchor{}, fmt.Errorf("cannot anchor tampered SIEM receiver term history term=%d", term)
	}
	if existing, found, err := repo.GetControlPlaneSIEMTermLedgerAnchor(ctx, term); err != nil {
		return existing, err
	} else if found {
		if existing.RecordHash != entry.RecordHash {
			return existing, fmt.Errorf("local SIEM term anchor conflicts with current history term=%d", term)
		}
		_ = repo.MarkControlPlaneSIEMTermAnchorDelivered(ctx, term, existing.AnchoredAt)
		return existing, nil
	}
	anchor, replicaReceipts, err := w.config.Governance.AnchorTermWithReplicaReceipts(ctx, entry)
	if len(replicaReceipts) > 0 {
		if receiptErr := repo.RecordControlPlaneSIEMGovernanceReplicaReceipts(ctx, replicaReceipts); receiptErr != nil {
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "conflict").Inc()
			return anchor, fmt.Errorf("persist governance replica anchor receipts: %w", receiptErr)
		}
		for _, receipt := range replicaReceipts {
			appmetrics.ControlPlaneSIEMGovernanceReplicaReceipts.WithLabelValues("anchor", receipt.ReplicaID, "verified").Inc()
		}
	}
	if err != nil {
		if w.config.Governance.HAEnabled() {
			outcome := "incomplete"
			if IsGovernanceConsistencyError(err) {
				outcome = "conflict"
			}
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", outcome).Inc()
		}
		appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("failed").Inc()
		return anchor, err
	}
	if w.config.Governance.HAEnabled() {
		appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "satisfied").Inc()
	}
	if err := repo.RecordControlPlaneSIEMTermLedgerAnchor(ctx, anchor); err != nil {
		return anchor, err
	}
	_ = repo.MarkControlPlaneSIEMTermAnchorDelivered(ctx, term, anchor.AnchoredAt)
	appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("anchored").Inc()
	return anchor, nil
}

func (w *DeliveryWorker) recordIncident(ctx context.Context, incidentID, kind, severity string, term uint64, active string, rpo uint64, rto float64, reason string) error {
	repo, err := w.promotionRepository()
	if err != nil {
		return err
	}
	evidenceBody, _ := json.Marshal(map[string]any{
		"incident_id":     incidentID,
		"kind":            kind,
		"term":            term,
		"active_receiver": active,
		"rpo_sequences":   rpo,
		"rto_seconds":     rto,
		"reason":          reason,
	})
	evidence, err := repo.CreateControlPlaneSIEMIncidentEvidence(ctx, domain.ControlPlaneSIEMIncidentEvidence{
		IncidentID:     incidentID,
		Kind:           kind,
		Severity:       severity,
		Term:           term,
		ActiveReceiver: active,
		RPOSequences:   rpo,
		RTOSeconds:     rto,
		Reason:         strings.TrimSpace(reason),
		EvidenceJSON:   string(evidenceBody),
		CreatedAt:      time.Now(),
	})
	if err != nil {
		return err
	}
	appmetrics.ControlPlaneSIEMIncidentEvidence.WithLabelValues(kind).Inc()
	if w.config.Governance != nil {
		replicaReceipts, alertErr := w.config.Governance.AlertIncidentWithReplicaReceipts(ctx, evidence)
		if len(replicaReceipts) > 0 {
			if receiptErr := repo.RecordControlPlaneSIEMGovernanceReplicaReceipts(ctx, replicaReceipts); receiptErr != nil {
				appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", "conflict").Inc()
				return fmt.Errorf("persist governance replica alert receipts: %w", receiptErr)
			}
			for _, receipt := range replicaReceipts {
				appmetrics.ControlPlaneSIEMGovernanceReplicaReceipts.WithLabelValues("alert", receipt.ReplicaID, "verified").Inc()
			}
		}
		if alertErr != nil {
			if w.config.Governance.HAEnabled() {
				outcome := "incomplete"
				if IsGovernanceConsistencyError(alertErr) {
					outcome = "conflict"
				}
				appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", outcome).Inc()
			}
			appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("failed").Inc()
			return alertErr
		}
		if w.config.Governance.HAEnabled() {
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", "satisfied").Inc()
		}
		if err := repo.MarkControlPlaneSIEMIncidentAlertDelivered(ctx, evidence.ID, time.Now()); err != nil {
			appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("failed").Inc()
			return err
		}
		appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("delivered").Inc()
	}
	return nil
}

func operatorHasRole(policy, operator, required string) bool {
	policy = strings.TrimSpace(policy)
	operator = strings.TrimSpace(operator)
	required = strings.TrimSpace(required)
	if policy == "" {
		return true
	}
	for _, assignment := range strings.Split(policy, ";") {
		parts := strings.SplitN(strings.TrimSpace(assignment), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) != operator {
			continue
		}
		for _, role := range strings.Split(parts[1], "|") {
			if strings.TrimSpace(role) == required {
				return true
			}
		}
	}
	return false
}

func (w *DeliveryWorker) ReceiverTermHistory(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMReceiverTermHistory, domain.ControlPlaneSIEMTermHistoryVerification, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, domain.ControlPlaneSIEMTermHistoryVerification{}, err
	}
	entries, err := repo.ListControlPlaneSIEMReceiverTermHistory(ctx, limit)
	if err != nil {
		return nil, domain.ControlPlaneSIEMTermHistoryVerification{}, err
	}
	report := verifyReceiverTermHistory(entries)
	if report.Valid {
		appmetrics.ControlPlaneSIEMTermHistoryIntegrity.Set(1)
	} else {
		appmetrics.ControlPlaneSIEMTermHistoryIntegrity.Set(0)
	}
	return entries, report, nil
}

func (w *DeliveryWorker) promoteReceiverTermCoordinated(
	ctx context.Context,
	target string,
	expectedTerm, intentID uint64,
	promotedBy, requestedBy, approvedBy, reason, action string,
	rpoSequences uint64,
	emergency bool,
	incidentID string,
) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	token, err := randomLeaseToken()
	if err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, fmt.Errorf("create SIEM promotion execution token: %w", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	state, err := repo.ClaimControlPlaneSIEMPromotionExecution(ctx, intentID, target, expectedTerm, token, now.Add(w.config.PromotionLease), now)
	if err != nil {
		appmetrics.ControlPlaneSIEMPromotionExecution.WithLabelValues("claim_failed").Inc()
		return state, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = repo.ReleaseControlPlaneSIEMPromotionExecution(context.Background(), intentID, token, time.Now())
		}
	}()
	candidate := expectedTerm + 1
	if err := w.fenceReceiverTerm(ctx, w.config.PrimaryTermEndpoint, w.config.PrimaryReceiver, target, candidate); err != nil {
		return state, fmt.Errorf("fence primary receiver at coordinated term=%d: %w", candidate, err)
	}
	if err := w.fenceReceiverTerm(ctx, w.config.SecondaryTermEndpoint, w.config.SecondaryReceiver, target, candidate); err != nil {
		return state, fmt.Errorf("fence secondary receiver at coordinated term=%d: %w", candidate, err)
	}
	promoted, err := repo.CommitControlPlaneSIEMPromotionExecutionV2(
		ctx,
		target,
		action,
		strings.TrimSpace(promotedBy),
		strings.TrimSpace(requestedBy),
		strings.TrimSpace(approvedBy),
		strings.TrimSpace(reason),
		expectedTerm,
		intentID,
		rpoSequences,
		token,
		emergency,
		incidentID,
		time.Now(),
	)
	if err != nil {
		return promoted, err
	}
	committed = true
	if promoted.Term != candidate || promoted.ActiveReceiver != target {
		return promoted, fmt.Errorf("coordinated SIEM promotion mismatch receiver=%s term=%d", promoted.ActiveReceiver, promoted.Term)
	}
	appmetrics.ControlPlaneSIEMReceiverTerm.Set(float64(promoted.Term))
	return promoted, nil
}

func (w *DeliveryWorker) observeRPOPolicy(ctx context.Context, rpo uint64) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return domain.ControlPlaneSIEMReceiverRoutingState{}, err
	}
	before, err := w.getReceiverRoutingState(ctx)
	if err != nil {
		return before, err
	}
	state, err := repo.ObserveControlPlaneSIEMReceiverRPO(ctx, rpo, w.config.RPOMaxSequences, w.config.RPOGrace, time.Now())
	if err != nil {
		return state, err
	}
	if state.RPOFenced {
		appmetrics.ControlPlaneSIEMRPOFence.Set(1)
		if !before.RPOFenced {
			appmetrics.ControlPlaneSIEMRPOBreaches.Inc()
			incidentID := fmt.Sprintf("rpo-term-%d-%d", state.Term, time.Now().UnixMilli())
			_ = w.recordIncident(ctx, incidentID, "rpo_breach", "critical", state.Term, state.ActiveReceiver, rpo, 0, state.RPOFenceReason)
		}
	} else {
		appmetrics.ControlPlaneSIEMRPOFence.Set(0)
		if before.RPOFenced {
			incidentID := fmt.Sprintf("rpo-recovered-term-%d-%d", state.Term, time.Now().UnixMilli())
			_ = w.recordIncident(ctx, incidentID, "rpo_recovered", "info", state.Term, state.ActiveReceiver, rpo, 0, "receiver RPO returned within policy")
		}
	}
	return state, nil
}

func (w *DeliveryWorker) promotionRepository() (receiverPromotionRepository, error) {
	repo, ok := w.repo.(receiverPromotionRepository)
	if !ok {
		return nil, fmt.Errorf("delivery repository does not support Phase 31 promotion workflow")
	}
	return repo, nil
}

func verifyReceiverTermHistory(entries []domain.ControlPlaneSIEMReceiverTermHistory) domain.ControlPlaneSIEMTermHistoryVerification {
	report := domain.ControlPlaneSIEMTermHistoryVerification{Valid: true, Entries: len(entries)}
	for i, entry := range entries {
		computed, err := domain.ComputeControlPlaneSIEMReceiverTermHistoryHash(entry)
		if err != nil || computed != entry.RecordHash {
			report.Valid = false
			report.FailedTerm = entry.Term
			report.FailureCause = "record hash mismatch"
			return report
		}
		if i == 0 {
			if entry.PreviousHash != "" {
				report.Valid = false
				report.FailedTerm = entry.Term
				report.FailureCause = "first retained term has unexpected previous hash"
				return report
			}
		} else {
			previous := entries[i-1]
			if entry.PreviousTerm != previous.Term || entry.PreviousReceiver != previous.ActiveReceiver || entry.PreviousHash != previous.RecordHash || entry.Term <= previous.Term {
				report.Valid = false
				report.FailedTerm = entry.Term
				report.FailureCause = "term history chain discontinuity"
				return report
			}
		}
		report.LastTerm = entry.Term
		report.LastHash = entry.RecordHash
	}
	return report
}
