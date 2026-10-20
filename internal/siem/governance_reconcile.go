package siem

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
)

func (w *DeliveryWorker) GovernanceStatus(ctx context.Context) (domain.ControlPlaneSIEMGovernanceStatus, error) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	status := domain.ControlPlaneSIEMGovernanceStatus{
		Enabled:        w.config.Governance != nil,
		PolicyEnabled:  w.config.GovernanceDebtPolicyEnabled,
		MaxOutstanding: w.config.GovernanceMaxOutstanding,
		MaxAgeSeconds:  w.config.GovernanceMaxAge.Seconds(),
		CheckedAt:      now,
	}
	if w.config.Governance == nil {
		w.refreshGovernanceMetrics(status)
		return status, nil
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return status, err
	}
	if expired, expireErr := repo.ExpireControlPlaneSIEMGovernanceRemediations(ctx, now); expireErr != nil {
		return status, expireErr
	} else if expired > 0 {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("expired").Add(float64(expired))
	}
	status, err = repo.ControlPlaneSIEMGovernanceDeliveryStatus(ctx, now)
	if err != nil {
		return status, err
	}
	status.Enabled = true
	status.PolicyEnabled = w.config.GovernanceDebtPolicyEnabled
	status.MaxOutstanding = w.config.GovernanceMaxOutstanding
	status.MaxAgeSeconds = w.config.GovernanceMaxAge.Seconds()
	status.HAEnabled = w.config.Governance.HAEnabled()
	status.ReplicaCount = w.config.Governance.ReplicaCount()
	status.ReplicaQuorum = w.config.Governance.Quorum()
	status.MembershipEpoch = w.config.Governance.MembershipEpoch()
	status.PreviousKeyGraceReplicas = w.config.Governance.PreviousKeyGraceCount(now)
	if status.HAEnabled {
		status.ReplicaReceiptDebt = status.Anchors.Pending + status.Anchors.Delivering + status.Alerts.Pending + status.Alerts.Delivering
	}
	if status.Dead > 0 {
		status.Fenced = true
		status.FenceReason = fmt.Sprintf("dead governance evidence=%d requires operator remediation", status.Dead)
	} else if status.HAEnabled && status.ReplicaReceiptDebt > 0 {
		status.Fenced = true
		status.FenceReason = fmt.Sprintf(
			"governance replica quorum incomplete outstanding=%d quorum=%d/%d",
			status.ReplicaReceiptDebt, status.ReplicaQuorum, status.ReplicaCount,
		)
	} else if status.PolicyEnabled {
		switch {
		case status.OpenRemediations > 0:
			status.Fenced = true
			status.FenceReason = fmt.Sprintf("governance remediation in progress=%d", status.OpenRemediations)
		case status.Outstanding > int64(w.config.GovernanceMaxOutstanding):
			status.Fenced = true
			status.FenceReason = fmt.Sprintf("governance outstanding=%d exceeds max=%d", status.Outstanding, w.config.GovernanceMaxOutstanding)
		case status.OldestOutstandingSeconds > w.config.GovernanceMaxAge.Seconds():
			status.Fenced = true
			status.FenceReason = fmt.Sprintf("oldest governance debt %.3fs exceeds max %.3fs", status.OldestOutstandingSeconds, w.config.GovernanceMaxAge.Seconds())
		}
	}
	status.FenceReason = strings.TrimSpace(status.FenceReason)
	w.refreshGovernanceMetrics(status)
	return status, nil
}

func (w *DeliveryWorker) refreshGovernanceMetrics(status domain.ControlPlaneSIEMGovernanceStatus) {
	classes := []struct {
		name  string
		stats domain.ControlPlaneSIEMGovernanceDeliveryClassStats
	}{
		{"anchor", status.Anchors},
		{"alert", status.Alerts},
	}
	for _, item := range classes {
		appmetrics.ControlPlaneSIEMGovernanceBacklog.WithLabelValues(item.name, "pending").Set(float64(item.stats.Pending))
		appmetrics.ControlPlaneSIEMGovernanceBacklog.WithLabelValues(item.name, "delivering").Set(float64(item.stats.Delivering))
		appmetrics.ControlPlaneSIEMGovernanceBacklog.WithLabelValues(item.name, "delivered").Set(float64(item.stats.Delivered))
		appmetrics.ControlPlaneSIEMGovernanceBacklog.WithLabelValues(item.name, "dead").Set(float64(item.stats.Dead))
		appmetrics.ControlPlaneSIEMGovernanceOldestOutstanding.WithLabelValues(item.name).Set(item.stats.OldestOutstandingSeconds)
	}
	if status.Fenced {
		appmetrics.ControlPlaneSIEMGovernanceDebtFence.Set(1)
	} else {
		appmetrics.ControlPlaneSIEMGovernanceDebtFence.Set(0)
	}
	appmetrics.ControlPlaneSIEMGovernanceOpenRemediations.Set(float64(status.OpenRemediations))
	appmetrics.ControlPlaneSIEMGovernancePendingApprovals.Set(float64(status.PendingRemediationApprovals))
	appmetrics.ControlPlaneSIEMGovernanceReplicaReceiptDebt.Set(float64(status.ReplicaReceiptDebt))
	appmetrics.ControlPlaneSIEMGovernancePreviousKeyGraceReplicas.Set(float64(status.PreviousKeyGraceReplicas))
}

func (w *DeliveryWorker) GovernanceDead(ctx context.Context, kind string, limit int) ([]domain.ControlPlaneSIEMGovernanceDeadItem, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMGovernanceDead(ctx, kind, limit)
}

func (w *DeliveryWorker) GovernanceRemediations(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMGovernanceRemediation, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMGovernanceRemediations(ctx, limit)
}

func (w *DeliveryWorker) GovernanceReplicaReceipts(
	ctx context.Context,
	kind string,
	targetID uint64,
	limit int,
) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	repo, err := w.promotionRepository()
	if err != nil {
		return nil, err
	}
	return repo.ListControlPlaneSIEMGovernanceReplicaReceipts(ctx, kind, targetID, limit)
}

func (w *DeliveryWorker) RequeueGovernanceDead(
	ctx context.Context,
	kind string,
	id uint64,
	reason, operator string,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	if w.config.Governance == nil {
		return remediation, fmt.Errorf("SIEM governance delivery is disabled")
	}
	if w.config.GovernanceRemediationApprovalRequired {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("approval_required").Inc()
		return remediation, fmt.Errorf("direct governance remediation requeue is disabled while approval workflow is required")
	}
	operator = strings.TrimSpace(operator)
	reason = strings.TrimSpace(reason)
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "incident-commander") {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("denied").Inc()
		return remediation, fmt.Errorf("operator %s lacks incident-commander role for governance remediation", operator)
	}
	if reason == "" {
		return remediation, fmt.Errorf("governance remediation reason is required")
	}
	if len([]rune(reason)) > 1000 {
		return remediation, fmt.Errorf("governance remediation reason exceeds 1000 characters")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return remediation, err
	}
	remediation, err = repo.RequeueControlPlaneSIEMGovernanceDead(ctx, kind, id, operator, reason, time.Now())
	if err != nil {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("failed").Inc()
		return remediation, err
	}
	appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("requeued").Inc()
	if _, statusErr := w.GovernanceStatus(ctx); statusErr != nil {
		log.Printf("refresh governance status after remediation id=%d: %v", remediation.ID, statusErr)
	}
	return remediation, nil
}

func (w *DeliveryWorker) RequestGovernanceRemediation(
	ctx context.Context,
	kind string,
	id uint64,
	operationKey, reason, operator string,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	if w.config.Governance == nil {
		return remediation, fmt.Errorf("SIEM governance delivery is disabled")
	}
	if !w.config.GovernanceRemediationApprovalRequired {
		return remediation, fmt.Errorf("governance remediation approval workflow is disabled")
	}
	operator = strings.TrimSpace(operator)
	reason = strings.TrimSpace(reason)
	operationKey = strings.TrimSpace(operationKey)
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "incident-commander") {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("request_denied").Inc()
		return remediation, fmt.Errorf("operator %s lacks incident-commander role for governance remediation request", operator)
	}
	if operationKey == "" {
		return remediation, fmt.Errorf("governance remediation operation_key is required")
	}
	if len(operationKey) > 128 {
		return remediation, fmt.Errorf("governance remediation operation_key exceeds 128 characters")
	}
	if reason == "" {
		return remediation, fmt.Errorf("governance remediation reason is required")
	}
	if len([]rune(reason)) > 1000 {
		return remediation, fmt.Errorf("governance remediation reason exceeds 1000 characters")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return remediation, err
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	expiresAt := now.Add(w.config.GovernanceRemediationApprovalTTL)
	remediation, idempotent, err := repo.RequestControlPlaneSIEMGovernanceRemediation(ctx, kind, id, operator, reason, operationKey, expiresAt, now)
	if err != nil {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("request_failed").Inc()
		return remediation, err
	}
	if idempotent {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("request_idempotent").Inc()
	} else {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("requested").Inc()
	}
	if _, statusErr := w.GovernanceStatus(ctx); statusErr != nil {
		log.Printf("refresh governance status after remediation request id=%d: %v", remediation.ID, statusErr)
	}
	return remediation, nil
}

func (w *DeliveryWorker) ApproveGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	note, operator string,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	if w.config.Governance == nil {
		return remediation, fmt.Errorf("SIEM governance delivery is disabled")
	}
	if !w.config.GovernanceRemediationApprovalRequired {
		return remediation, fmt.Errorf("governance remediation approval workflow is disabled")
	}
	operator = strings.TrimSpace(operator)
	note = strings.TrimSpace(note)
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "governance-reviewer") {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("approval_denied").Inc()
		return remediation, fmt.Errorf("operator %s lacks governance-reviewer role", operator)
	}
	if note == "" {
		return remediation, fmt.Errorf("governance remediation approval note is required")
	}
	if len([]rune(note)) > 1000 {
		return remediation, fmt.Errorf("governance remediation approval note exceeds 1000 characters")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return remediation, err
	}
	remediation, idempotent, err := repo.ApproveControlPlaneSIEMGovernanceRemediation(ctx, remediationID, operator, note, w.config.GovernanceRemediationStaleTargetFence, time.Now())
	if err != nil {
		switch remediation.State {
		case "stale":
			appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("stale").Inc()
		case "expired":
			appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("expired").Inc()
		default:
			appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("approval_failed").Inc()
		}
		return remediation, err
	}
	if idempotent {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("approval_idempotent").Inc()
	} else {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("approved").Inc()
	}
	if _, statusErr := w.GovernanceStatus(ctx); statusErr != nil {
		log.Printf("refresh governance status after remediation approval id=%d: %v", remediation.ID, statusErr)
	}
	return remediation, nil
}

func (w *DeliveryWorker) RejectGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	note, operator string,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	if w.config.Governance == nil || !w.config.GovernanceRemediationApprovalRequired {
		return remediation, fmt.Errorf("governance remediation approval workflow is disabled")
	}
	operator = strings.TrimSpace(operator)
	note = strings.TrimSpace(note)
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "governance-reviewer") {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("rejection_denied").Inc()
		return remediation, fmt.Errorf("operator %s lacks governance-reviewer role", operator)
	}
	if note == "" || len([]rune(note)) > 1000 {
		return remediation, fmt.Errorf("governance remediation rejection note is required and must not exceed 1000 characters")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return remediation, err
	}
	remediation, idempotent, err := repo.RejectControlPlaneSIEMGovernanceRemediation(ctx, remediationID, operator, note, time.Now())
	if err != nil {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("rejection_failed").Inc()
		return remediation, err
	}
	if idempotent {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("rejection_idempotent").Inc()
	} else {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("rejected").Inc()
	}
	return remediation, nil
}

func (w *DeliveryWorker) CancelGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	note, operator string,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	if w.config.Governance == nil || !w.config.GovernanceRemediationApprovalRequired {
		return remediation, fmt.Errorf("governance remediation approval workflow is disabled")
	}
	operator = strings.TrimSpace(operator)
	note = strings.TrimSpace(note)
	if !operatorHasRole(w.config.PromotionRolePolicy, operator, "incident-commander") {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("cancellation_denied").Inc()
		return remediation, fmt.Errorf("operator %s lacks incident-commander role", operator)
	}
	if note == "" || len([]rune(note)) > 1000 {
		return remediation, fmt.Errorf("governance remediation cancellation note is required and must not exceed 1000 characters")
	}
	repo, err := w.promotionRepository()
	if err != nil {
		return remediation, err
	}
	remediation, idempotent, err := repo.CancelControlPlaneSIEMGovernanceRemediation(ctx, remediationID, operator, note, time.Now())
	if err != nil {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("cancellation_failed").Inc()
		return remediation, err
	}
	if idempotent {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("cancellation_idempotent").Inc()
	} else {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues("cancelled").Inc()
	}
	return remediation, nil
}

func (w *DeliveryWorker) reconcileGovernance(ctx context.Context) {
	if w.config.Governance == nil || ctx.Err() != nil {
		return
	}
	repo, err := w.promotionRepository()
	if err != nil {
		log.Printf("reconcile SIEM governance deliveries: %v", err)
		return
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	leaseToken, err := randomLeaseToken()
	if err != nil {
		log.Printf("create SIEM governance lease token: %v", err)
		return
	}
	leaseUntil := now.Add(w.config.LeaseDuration)
	limit := w.config.BatchSize
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	anchors, err := repo.ClaimControlPlaneSIEMTermAnchorDeliveries(ctx, now, leaseUntil, leaseToken, limit)
	if err != nil {
		appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_claim_failed").Inc()
		log.Printf("claim SIEM governance term anchors: %v", err)
	} else {
		for _, entry := range anchors {
			w.reconcileTermAnchor(ctx, repo, entry, leaseToken, now)
		}
	}

	alerts, err := repo.ClaimControlPlaneSIEMIncidentAlertDeliveries(ctx, now, leaseUntil, leaseToken, limit)
	if err != nil {
		appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("retry_claim_failed").Inc()
		log.Printf("claim SIEM governance incident alerts: %v", err)
		return
	}
	for _, evidence := range alerts {
		w.reconcileIncidentAlert(ctx, repo, evidence, leaseToken, now)
	}
	if _, err := w.GovernanceStatus(ctx); err != nil {
		log.Printf("refresh SIEM governance debt status: %v", err)
	}
}

func (w *DeliveryWorker) reconcileTermAnchor(
	ctx context.Context,
	repo receiverPromotionRepository,
	entry domain.ControlPlaneSIEMReceiverTermHistory,
	leaseToken string,
	now time.Time,
) {
	computed, err := domain.ComputeControlPlaneSIEMReceiverTermHistoryHash(entry)
	if err != nil || computed != entry.RecordHash {
		if err == nil {
			err = fmt.Errorf("term history hash mismatch term=%d", entry.Term)
		}
		_, settleErr := repo.FailControlPlaneSIEMTermAnchorDelivery(ctx, entry, leaseToken, now, now, err, 1)
		if settleErr != nil {
			log.Printf("dead-letter SIEM governance term anchor term=%d: %v", entry.Term, settleErr)
		} else {
			w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "failed", err.Error())
		}
		appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_dead").Inc()
		return
	}

	if existing, found, getErr := repo.GetControlPlaneSIEMTermLedgerAnchor(ctx, entry.Term); getErr != nil {
		w.failTermAnchorRetry(ctx, repo, entry, leaseToken, now, getErr)
		return
	} else if found {
		if existing.RecordHash != entry.RecordHash {
			conflictErr := fmt.Errorf("local term anchor conflicts with history term=%d", entry.Term)
			_, settleErr := repo.FailControlPlaneSIEMTermAnchorDelivery(ctx, entry, leaseToken, now, now, conflictErr, 1)
			if settleErr != nil {
				log.Printf("dead-letter conflicting SIEM governance anchor term=%d: %v", entry.Term, settleErr)
			} else {
				w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "failed", conflictErr.Error())
			}
			appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_dead").Inc()
			return
		}
		if err := repo.MarkControlPlaneSIEMTermAnchorDelivered(ctx, entry.Term, existing.AnchoredAt); err != nil {
			log.Printf("mark recovered SIEM governance anchor term=%d: %v", entry.Term, err)
			return
		}
		w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "resolved", "anchor already present and matches immutable term history")
		appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("reconciled").Inc()
		return
	}

	anchor, replicaReceipts, err := w.config.Governance.AnchorTermWithReplicaReceipts(ctx, entry)
	if len(replicaReceipts) > 0 {
		if receiptErr := repo.RecordControlPlaneSIEMGovernanceReplicaReceipts(ctx, replicaReceipts); receiptErr != nil {
			if !strings.Contains(receiptErr.Error(), "governance replica receipt conflict") {
				w.failTermAnchorRetry(ctx, repo, entry, leaseToken, now, fmt.Errorf("persist governance replica anchor receipts: %w", receiptErr))
				return
			}
			conflictErr := fmt.Errorf("persist governance replica anchor receipts: %w", receiptErr)
			_, settleErr := repo.FailControlPlaneSIEMTermAnchorDelivery(ctx, entry, leaseToken, now, now, conflictErr, 1)
			if settleErr != nil {
				log.Printf("dead-letter conflicting governance replica receipt term=%d: %v", entry.Term, settleErr)
			} else {
				w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "failed", conflictErr.Error())
			}
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "conflict").Inc()
			appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_dead").Inc()
			return
		}
		for _, receipt := range replicaReceipts {
			appmetrics.ControlPlaneSIEMGovernanceReplicaReceipts.WithLabelValues("anchor", receipt.ReplicaID, "verified").Inc()
		}
	}
	if err != nil {
		if IsGovernanceConsistencyError(err) {
			_, settleErr := repo.FailControlPlaneSIEMTermAnchorDelivery(ctx, entry, leaseToken, now, now, err, 1)
			if settleErr != nil {
				log.Printf("dead-letter governance replica consistency failure term=%d: %v", entry.Term, settleErr)
			} else {
				w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "failed", err.Error())
			}
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "conflict").Inc()
			appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_dead").Inc()
			return
		}
		if w.config.Governance.HAEnabled() {
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "incomplete").Inc()
		}
		w.failTermAnchorRetry(ctx, repo, entry, leaseToken, now, err)
		return
	}
	if w.config.Governance.HAEnabled() {
		appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("anchor", "satisfied").Inc()
	}
	if err := repo.RecordControlPlaneSIEMTermLedgerAnchor(ctx, anchor); err != nil {
		w.failTermAnchorRetry(ctx, repo, entry, leaseToken, now, err)
		return
	}
	if err := repo.MarkControlPlaneSIEMTermAnchorDelivered(ctx, entry.Term, anchor.AnchoredAt); err != nil {
		log.Printf("mark delivered SIEM governance anchor term=%d: %v", entry.Term, err)
		return
	}
	w.settleGovernanceRemediation(ctx, repo, "anchor", entry.ID, "resolved", "anchor delivered to governance endpoint")
	appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("reconciled").Inc()
}

func (w *DeliveryWorker) failTermAnchorRetry(
	ctx context.Context,
	repo receiverPromotionRepository,
	entry domain.ControlPlaneSIEMReceiverTermHistory,
	leaseToken string,
	now time.Time,
	cause error,
) {
	retryAt := now.Add(retryDelay(w.config.RetryBase, w.config.RetryMax, entry.AnchorAttempts))
	dead, err := repo.FailControlPlaneSIEMTermAnchorDelivery(ctx, entry, leaseToken, now, retryAt, cause, 0)
	if err != nil {
		log.Printf("settle SIEM governance anchor retry term=%d: %v", entry.Term, err)
		return
	}
	if dead {
		appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_dead").Inc()
		return
	}
	appmetrics.ControlPlaneSIEMTermAnchors.WithLabelValues("retry_scheduled").Inc()
}

func (w *DeliveryWorker) reconcileIncidentAlert(
	ctx context.Context,
	repo receiverPromotionRepository,
	evidence domain.ControlPlaneSIEMIncidentEvidence,
	leaseToken string,
	now time.Time,
) {
	replicaReceipts, err := w.config.Governance.AlertIncidentWithReplicaReceipts(ctx, evidence)
	if len(replicaReceipts) > 0 {
		if receiptErr := repo.RecordControlPlaneSIEMGovernanceReplicaReceipts(ctx, replicaReceipts); receiptErr != nil {
			err = fmt.Errorf("persist governance replica alert receipts: %w", receiptErr)
		} else {
			for _, receipt := range replicaReceipts {
				appmetrics.ControlPlaneSIEMGovernanceReplicaReceipts.WithLabelValues("alert", receipt.ReplicaID, "verified").Inc()
			}
		}
	}
	if err == nil {
		if w.config.Governance.HAEnabled() {
			appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", "satisfied").Inc()
		}
		if markErr := repo.MarkControlPlaneSIEMIncidentAlertDelivered(ctx, evidence.ID, time.Now()); markErr != nil {
			log.Printf("mark delivered SIEM governance incident evidence=%d: %v", evidence.ID, markErr)
			return
		}
		w.settleGovernanceRemediation(ctx, repo, "alert", evidence.ID, "resolved", "incident alert delivered to governance endpoint")
		appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("reconciled").Inc()
		return
	}
	if IsGovernanceConsistencyError(err) || strings.Contains(err.Error(), "governance replica receipt conflict") {
		_, settleErr := repo.FailControlPlaneSIEMIncidentAlertDelivery(ctx, evidence, leaseToken, now, now, err, 1)
		if settleErr != nil {
			log.Printf("dead-letter governance replica consistency failure evidence=%d: %v", evidence.ID, settleErr)
		} else {
			w.settleGovernanceRemediation(ctx, repo, "alert", evidence.ID, "failed", err.Error())
		}
		appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", "conflict").Inc()
		appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("retry_dead").Inc()
		return
	}
	if w.config.Governance.HAEnabled() {
		appmetrics.ControlPlaneSIEMGovernanceQuorum.WithLabelValues("alert", "incomplete").Inc()
	}

	retryAt := now.Add(retryDelay(w.config.RetryBase, w.config.RetryMax, evidence.AlertAttempts))
	dead, settleErr := repo.FailControlPlaneSIEMIncidentAlertDelivery(ctx, evidence, leaseToken, now, retryAt, err, 0)
	if settleErr != nil {
		log.Printf("settle SIEM governance incident retry evidence=%d: %v", evidence.ID, settleErr)
		return
	}
	if dead {
		w.settleGovernanceRemediation(ctx, repo, "alert", evidence.ID, "failed", err.Error())
		appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("retry_dead").Inc()
		return
	}
	appmetrics.ControlPlaneSIEMIncidentAlerts.WithLabelValues("retry_scheduled").Inc()
}

func (w *DeliveryWorker) settleGovernanceRemediation(
	ctx context.Context,
	repo receiverPromotionRepository,
	kind string,
	targetID uint64,
	state, result string,
) {
	settled, err := repo.SettleControlPlaneSIEMGovernanceRemediations(ctx, kind, targetID, state, result, time.Now())
	if err != nil {
		log.Printf("settle SIEM governance remediation kind=%s target=%d state=%s: %v", kind, targetID, state, err)
		return
	}
	if settled {
		appmetrics.ControlPlaneSIEMGovernanceRemediations.WithLabelValues(state).Inc()
	}
}
