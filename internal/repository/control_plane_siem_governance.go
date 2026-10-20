package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *TaskRepository) ApproveControlPlaneSIEMPromotionIntentWithQuorum(
	ctx context.Context,
	id uint64,
	approvedBy, role, note string,
	requiredApprovals int,
	now time.Time,
) (domain.ControlPlaneSIEMPromotionIntent, error) {
	approvedBy = strings.TrimSpace(approvedBy)
	role = strings.TrimSpace(role)
	note = strings.TrimSpace(note)
	if id == 0 || approvedBy == "" || role == "" || requiredApprovals <= 0 {
		return domain.ControlPlaneSIEMPromotionIntent{}, fmt.Errorf("promotion intent id, approver role and positive quorum are required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	var intent domain.ControlPlaneSIEMPromotionIntent
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrControlPlaneSIEMPromotionIntentNotFound
			}
			return err
		}
		if intent.State == domain.ControlPlaneSIEMPromotionPending && !intent.ExpiresAt.After(now) {
			intent.State = domain.ControlPlaneSIEMPromotionExpired
			intent.UpdatedAt = now
			if err := tx.Save(&intent).Error; err != nil {
				return err
			}
			return fmt.Errorf("%w: promotion intent expired", ErrControlPlaneSIEMPromotionConflict)
		}
		if intent.State != domain.ControlPlaneSIEMPromotionPending {
			return fmt.Errorf("%w: promotion intent state=%s", ErrControlPlaneSIEMPromotionConflict, intent.State)
		}
		if intent.RequestedBy == approvedBy {
			return fmt.Errorf("%w: requester and approver must be different operators", ErrControlPlaneSIEMPromotionApproval)
		}
		approval := domain.ControlPlaneSIEMPromotionApproval{
			IntentID:   id,
			OperatorID: approvedBy,
			Role:       role,
			Note:       note,
			ApprovedAt: now,
		}
		if err := tx.Create(&approval).Error; err != nil {
			return fmt.Errorf("%w: duplicate or invalid approval", ErrControlPlaneSIEMPromotionApproval)
		}
		var approvals []domain.ControlPlaneSIEMPromotionApproval
		if err := tx.Where("intent_id = ?", id).Order("operator_id ASC").Find(&approvals).Error; err != nil {
			return err
		}
		operators := make([]string, 0, len(approvals))
		for _, item := range approvals {
			operators = append(operators, item.OperatorID)
		}
		sort.Strings(operators)
		intent.RequiredApprovals = requiredApprovals
		intent.ApprovalCount = len(operators)
		intent.ApprovedBy = strings.Join(operators, ",")
		intent.ApprovalNote = note
		if len(operators) >= requiredApprovals {
			approvedAt := now
			intent.State = domain.ControlPlaneSIEMPromotionApproved
			intent.ApprovedAt = &approvedAt
		}
		intent.UpdatedAt = now
		return tx.Save(&intent).Error
	})
	return intent, err
}

func (r *TaskRepository) ListControlPlaneSIEMPromotionApprovals(ctx context.Context, intentID uint64) ([]domain.ControlPlaneSIEMPromotionApproval, error) {
	var approvals []domain.ControlPlaneSIEMPromotionApproval
	err := r.db.WithContext(ctx).Where("intent_id = ?", intentID).Order("operator_id ASC").Find(&approvals).Error
	return approvals, err
}

func (r *TaskRepository) CancelActiveControlPlaneSIEMPromotionIntents(ctx context.Context, expectedTerm uint64, now time.Time) (int64, error) {
	now = now.UTC().Truncate(time.Millisecond)
	result := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMPromotionIntent{}).
		Where("expected_term = ? AND state IN ?", expectedTerm, []string{domain.ControlPlaneSIEMPromotionPending, domain.ControlPlaneSIEMPromotionApproved}).
		Updates(map[string]any{"state": domain.ControlPlaneSIEMPromotionCancelled, "updated_at": now})
	return result.RowsAffected, result.Error
}

func (r *TaskRepository) CommitControlPlaneSIEMPromotionExecutionV2(
	ctx context.Context,
	target, action, promotedBy, requestedBy, approvedBy, reason string,
	expectedTerm, intentID, rpoSequences uint64,
	token string,
	emergency bool,
	incidentID string,
	promotedAt time.Time,
) (domain.ControlPlaneSIEMReceiverRoutingState, error) {
	target = strings.TrimSpace(target)
	action = strings.TrimSpace(action)
	promotedBy = strings.TrimSpace(promotedBy)
	token = strings.TrimSpace(token)
	incidentID = strings.TrimSpace(incidentID)
	promotedAt = promotedAt.UTC().Truncate(time.Millisecond)
	var state domain.ControlPlaneSIEMReceiverRoutingState
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, 1).Error; err != nil {
			return err
		}
		if state.Term != expectedTerm {
			return fmt.Errorf("%w: current term=%d expected=%d", ErrControlPlaneSIEMPromotionConflict, state.Term, expectedTerm)
		}
		if state.TransitionToken != token || state.TransitionIntentID != intentID || state.TransitionLeaseUntil == nil || !state.TransitionLeaseUntil.After(promotedAt) {
			return ErrControlPlaneSIEMPromotionBusy
		}
		previousReceiver := state.ActiveReceiver
		previousTerm := state.Term
		state.ActiveReceiver = target
		state.Term++
		state.PromotedBy = promotedBy
		state.PromotedAt = promotedAt
		state.TransitionIntentID = 0
		state.TransitionToken = ""
		state.TransitionLeaseUntil = nil
		state.UpdatedAt = promotedAt
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		entry := domain.ControlPlaneSIEMReceiverTermHistory{
			Term:             state.Term,
			PreviousTerm:     previousTerm,
			PreviousReceiver: previousReceiver,
			ActiveReceiver:   state.ActiveReceiver,
			Action:           action,
			IntentID:         intentID,
			RequestedBy:      strings.TrimSpace(requestedBy),
			ApprovedBy:       strings.TrimSpace(approvedBy),
			Reason:           strings.TrimSpace(reason),
			RPOSequences:     rpoSequences,
			HashVersion:      2,
			Emergency:        emergency,
			IncidentID:       incidentID,
			CreatedAt:        promotedAt,
		}
		if err := appendControlPlaneSIEMTermHistoryTx(tx, &entry); err != nil {
			return err
		}
		if intentID > 0 {
			var intent domain.ControlPlaneSIEMPromotionIntent
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&intent, intentID).Error; err != nil {
				return err
			}
			if intent.State != domain.ControlPlaneSIEMPromotionExecuting {
				return fmt.Errorf("%w: promotion intent state=%s", ErrControlPlaneSIEMPromotionConflict, intent.State)
			}
			executedAt := promotedAt
			intent.State = domain.ControlPlaneSIEMPromotionExecuted
			intent.ResultTerm = state.Term
			intent.ExecutedAt = &executedAt
			intent.UpdatedAt = promotedAt
			if err := tx.Save(&intent).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return state, err
}

func (r *TaskRepository) GetControlPlaneSIEMReceiverTermHistoryByTerm(ctx context.Context, term uint64) (domain.ControlPlaneSIEMReceiverTermHistory, error) {
	var entry domain.ControlPlaneSIEMReceiverTermHistory
	err := r.db.WithContext(ctx).Where("term = ?", term).First(&entry).Error
	return entry, err
}

func (r *TaskRepository) RecordControlPlaneSIEMTermLedgerAnchor(ctx context.Context, anchor domain.ControlPlaneSIEMTermLedgerAnchor) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&anchor).Error
}

func (r *TaskRepository) GetControlPlaneSIEMTermLedgerAnchor(ctx context.Context, term uint64) (domain.ControlPlaneSIEMTermLedgerAnchor, bool, error) {
	var anchor domain.ControlPlaneSIEMTermLedgerAnchor
	err := r.db.WithContext(ctx).Where("term = ?", term).First(&anchor).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return anchor, false, nil
	}
	return anchor, err == nil, err
}

func (r *TaskRepository) RecordControlPlaneSIEMGovernanceReplicaReceipts(
	ctx context.Context,
	receipts []domain.ControlPlaneSIEMGovernanceReplicaReceipt,
) error {
	if len(receipts) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, receipt := range receipts {
			receipt.Kind = strings.TrimSpace(strings.ToLower(receipt.Kind))
			receipt.ReplicaID = strings.TrimSpace(receipt.ReplicaID)
			receipt.Identity = strings.TrimSpace(receipt.Identity)
			receipt.ReceiptKeyState = strings.TrimSpace(strings.ToLower(receipt.ReceiptKeyState))
			if (receipt.Kind != "anchor" && receipt.Kind != "alert") || receipt.TargetID == 0 || receipt.ReplicaID == "" || receipt.Identity == "" || len(receipt.ReceiptHash) != 64 || receipt.MembershipEpoch == 0 || receipt.ReceiptKeyGeneration == 0 || (receipt.ReceiptKeyState != "active" && receipt.ReceiptKeyState != "previous") {
				return fmt.Errorf("invalid governance replica receipt")
			}
			var existing domain.ControlPlaneSIEMGovernanceReplicaReceipt
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("kind = ? AND target_id = ? AND replica_id = ?", receipt.Kind, receipt.TargetID, receipt.ReplicaID).
				First(&existing).Error
			if err == nil {
				coreMatches := existing.Identity == receipt.Identity && existing.RemoteID == receipt.RemoteID && existing.ReceiptHash == receipt.ReceiptHash &&
					existing.ReceiptKeyID == receipt.ReceiptKeyID && existing.ReceiptAlgorithm == receipt.ReceiptAlgorithm && existing.ReceiptSignature == receipt.ReceiptSignature &&
					existing.MembershipEpoch == receipt.MembershipEpoch
				if coreMatches && existing.ReceiptKeyGeneration == 0 && strings.TrimSpace(existing.ReceiptKeyState) == "" {
					if receipt.ReceiptKeyState == "active" {
						if err := enforceGovernanceReplicaActiveKeyGeneration(tx, receipt); err != nil {
							return err
						}
					}
					if err := tx.Model(&domain.ControlPlaneSIEMGovernanceReplicaReceipt{}).
						Where("id = ?", existing.ID).
						Updates(map[string]any{"receipt_key_generation": receipt.ReceiptKeyGeneration, "receipt_key_state": receipt.ReceiptKeyState}).Error; err != nil {
						return err
					}
					continue
				}
				if !coreMatches || existing.ReceiptKeyGeneration != receipt.ReceiptKeyGeneration || existing.ReceiptKeyState != receipt.ReceiptKeyState {
					return fmt.Errorf("governance replica receipt conflict kind=%s target=%d replica=%s", receipt.Kind, receipt.TargetID, receipt.ReplicaID)
				}
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if receipt.ReceiptKeyState == "active" {
				if err := enforceGovernanceReplicaActiveKeyGeneration(tx, receipt); err != nil {
					return err
				}
			}
			if receipt.CreatedAt.IsZero() {
				receipt.CreatedAt = time.Now().UTC().Truncate(time.Millisecond)
			}
			if receipt.ReceivedAt.IsZero() {
				receipt.ReceivedAt = receipt.CreatedAt
			}
			if err := tx.Create(&receipt).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func enforceGovernanceReplicaActiveKeyGeneration(tx *gorm.DB, receipt domain.ControlPlaneSIEMGovernanceReplicaReceipt) error {
	now := time.Now().UTC().Truncate(time.Millisecond)
	seed := domain.ControlPlaneSIEMGovernanceReplicaKeyState{ReplicaID: receipt.ReplicaID, UpdatedAt: now}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
		return err
	}
	var state domain.ControlPlaneSIEMGovernanceReplicaKeyState
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("replica_id = ?", receipt.ReplicaID).First(&state).Error; err != nil {
		return err
	}
	if receipt.ReceiptKeyGeneration < state.HighestActiveGeneration {
		return fmt.Errorf(
			"governance replica receipt conflict key generation rollback replica=%s got=%d floor=%d",
			receipt.ReplicaID, receipt.ReceiptKeyGeneration, state.HighestActiveGeneration,
		)
	}
	if receipt.ReceiptKeyGeneration == state.HighestActiveGeneration && state.ActiveKeyID != "" && state.ActiveKeyID != receipt.ReceiptKeyID {
		return fmt.Errorf(
			"governance replica receipt conflict key generation reuse replica=%s generation=%d active_key_id=%s got_key_id=%s",
			receipt.ReplicaID, receipt.ReceiptKeyGeneration, state.ActiveKeyID, receipt.ReceiptKeyID,
		)
	}
	if receipt.ReceiptKeyGeneration > state.HighestActiveGeneration || state.ActiveKeyID == "" {
		return tx.Model(&domain.ControlPlaneSIEMGovernanceReplicaKeyState{}).
			Where("replica_id = ?", receipt.ReplicaID).
			Updates(map[string]any{
				"highest_active_generation": receipt.ReceiptKeyGeneration,
				"active_key_id":             receipt.ReceiptKeyID,
				"updated_at":                now,
			}).Error
	}
	return nil
}

func (r *TaskRepository) ListControlPlaneSIEMGovernanceReplicaReceipts(
	ctx context.Context,
	kind string,
	targetID uint64,
	limit int,
) ([]domain.ControlPlaneSIEMGovernanceReplicaReceipt, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	query := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceReplicaReceipt{})
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind != "" && kind != "all" {
		if kind != "anchor" && kind != "alert" {
			return nil, fmt.Errorf("governance replica receipt kind must be anchor or alert")
		}
		query = query.Where("kind = ?", kind)
	}
	if targetID > 0 {
		query = query.Where("target_id = ?", targetID)
	}
	var rows []domain.ControlPlaneSIEMGovernanceReplicaReceipt
	err := query.Order("id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

func (r *TaskRepository) CreateControlPlaneSIEMIncidentEvidence(ctx context.Context, evidence domain.ControlPlaneSIEMIncidentEvidence) (domain.ControlPlaneSIEMIncidentEvidence, error) {
	canonical := struct {
		IncidentID     string  `json:"incident_id"`
		Kind           string  `json:"kind"`
		Severity       string  `json:"severity"`
		Term           uint64  `json:"term"`
		ActiveReceiver string  `json:"active_receiver"`
		RPOSequences   uint64  `json:"rpo_sequences"`
		RTOSeconds     float64 `json:"rto_seconds"`
		Reason         string  `json:"reason"`
		EvidenceJSON   string  `json:"evidence_json"`
	}{evidence.IncidentID, evidence.Kind, evidence.Severity, evidence.Term, evidence.ActiveReceiver, evidence.RPOSequences, evidence.RTOSeconds, evidence.Reason, evidence.EvidenceJSON}
	body, err := json.Marshal(canonical)
	if err != nil {
		return evidence, err
	}
	digest := sha256.Sum256(body)
	evidence.EvidenceHash = hex.EncodeToString(digest[:])
	evidence.CreatedAt = evidence.CreatedAt.UTC().Truncate(time.Millisecond)
	err = r.db.WithContext(ctx).Create(&evidence).Error
	return evidence, err
}

func (r *TaskRepository) ListControlPlaneSIEMIncidentEvidence(ctx context.Context, limit int) ([]domain.ControlPlaneSIEMIncidentEvidence, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []domain.ControlPlaneSIEMIncidentEvidence
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

func (r *TaskRepository) ControlPlaneSIEMGovernanceDeliveryStatus(ctx context.Context, now time.Time) (domain.ControlPlaneSIEMGovernanceStatus, error) {
	now = now.UTC().Truncate(time.Millisecond)
	anchors, err := r.controlPlaneSIEMGovernanceClassStats(ctx, &domain.ControlPlaneSIEMReceiverTermHistory{}, "anchor_state", now)
	if err != nil {
		return domain.ControlPlaneSIEMGovernanceStatus{}, err
	}
	alerts, err := r.controlPlaneSIEMGovernanceClassStats(ctx, &domain.ControlPlaneSIEMIncidentEvidence{}, "alert_state", now)
	if err != nil {
		return domain.ControlPlaneSIEMGovernanceStatus{}, err
	}
	oldest := anchors.OldestOutstandingSeconds
	if alerts.OldestOutstandingSeconds > oldest {
		oldest = alerts.OldestOutstandingSeconds
	}
	var openRemediations int64
	if err := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceRemediation{}).
		Where("state = ?", "open").Count(&openRemediations).Error; err != nil {
		return domain.ControlPlaneSIEMGovernanceStatus{}, err
	}
	var pendingApprovals int64
	if err := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceRemediation{}).
		Where("state = ?", "pending_approval").Count(&pendingApprovals).Error; err != nil {
		return domain.ControlPlaneSIEMGovernanceStatus{}, err
	}
	var verifiedReplicaReceipts int64
	if err := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceReplicaReceipt{}).
		Count(&verifiedReplicaReceipts).Error; err != nil {
		return domain.ControlPlaneSIEMGovernanceStatus{}, err
	}
	return domain.ControlPlaneSIEMGovernanceStatus{
		Anchors:                     anchors,
		Alerts:                      alerts,
		Outstanding:                 anchors.Outstanding + alerts.Outstanding,
		Dead:                        anchors.Dead + alerts.Dead,
		OpenRemediations:            openRemediations,
		PendingRemediationApprovals: pendingApprovals,
		VerifiedReplicaReceipts:     verifiedReplicaReceipts,
		OldestOutstandingSeconds:    oldest,
		CheckedAt:                   now,
	}, nil
}

func (r *TaskRepository) controlPlaneSIEMGovernanceClassStats(
	ctx context.Context,
	model any,
	stateColumn string,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceDeliveryClassStats, error) {
	var stats domain.ControlPlaneSIEMGovernanceDeliveryClassStats
	counts := []struct {
		target *int64
		query  string
		args   []any
	}{
		{&stats.Pending, stateColumn + " IN ?", []any{[]string{"", domain.ControlPlaneDeliveryPending}}},
		{&stats.Delivering, stateColumn + " = ?", []any{domain.ControlPlaneDeliveryDelivering}},
		{&stats.Delivered, stateColumn + " = ?", []any{domain.ControlPlaneDeliveryDelivered}},
		{&stats.Dead, stateColumn + " = ?", []any{domain.ControlPlaneDeliveryDead}},
	}
	for _, item := range counts {
		if err := r.db.WithContext(ctx).Model(model).Where(item.query, item.args...).Count(item.target).Error; err != nil {
			return stats, err
		}
	}
	stats.Outstanding = stats.Pending + stats.Delivering + stats.Dead
	if stats.Outstanding == 0 {
		return stats, nil
	}
	var oldest struct {
		CreatedAt *time.Time `gorm:"column:created_at"`
	}
	if err := r.db.WithContext(ctx).Model(model).
		Select("MIN(created_at) AS created_at").
		Where(stateColumn+" <> ?", domain.ControlPlaneDeliveryDelivered).
		Scan(&oldest).Error; err != nil {
		return stats, err
	}
	if oldest.CreatedAt != nil {
		age := now.Sub(oldest.CreatedAt.UTC()).Seconds()
		if age > 0 {
			stats.OldestOutstandingSeconds = age
		}
	}
	return stats, nil
}

func (r *TaskRepository) ListControlPlaneSIEMGovernanceDead(
	ctx context.Context,
	kind string,
	limit int,
) ([]domain.ControlPlaneSIEMGovernanceDeadItem, error) {
	kind = strings.TrimSpace(strings.ToLower(kind))
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	switch kind {
	case "", "all":
		anchors, err := r.ListControlPlaneSIEMGovernanceDead(ctx, "anchor", limit)
		if err != nil {
			return nil, err
		}
		alerts, err := r.ListControlPlaneSIEMGovernanceDead(ctx, "alert", limit)
		if err != nil {
			return nil, err
		}
		items := append(anchors, alerts...)
		sort.Slice(items, func(i, j int) bool {
			if items[i].CreatedAt.Equal(items[j].CreatedAt) {
				return items[i].ID > items[j].ID
			}
			return items[i].CreatedAt.After(items[j].CreatedAt)
		})
		if len(items) > limit {
			items = items[:limit]
		}
		return items, nil
	case "anchor":
		var rows []domain.ControlPlaneSIEMReceiverTermHistory
		if err := r.db.WithContext(ctx).
			Where("anchor_state = ?", domain.ControlPlaneDeliveryDead).
			Order("id DESC").Limit(limit).Find(&rows).Error; err != nil {
			return nil, err
		}
		items := make([]domain.ControlPlaneSIEMGovernanceDeadItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, domain.ControlPlaneSIEMGovernanceDeadItem{
				Kind: "anchor", ID: row.ID, Term: row.Term, Identity: row.RecordHash,
				Attempts: row.AnchorAttempts, LastError: row.AnchorLastError, CreatedAt: row.CreatedAt,
			})
		}
		return items, nil
	case "alert":
		var rows []domain.ControlPlaneSIEMIncidentEvidence
		if err := r.db.WithContext(ctx).
			Where("alert_state = ?", domain.ControlPlaneDeliveryDead).
			Order("id DESC").Limit(limit).Find(&rows).Error; err != nil {
			return nil, err
		}
		items := make([]domain.ControlPlaneSIEMGovernanceDeadItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, domain.ControlPlaneSIEMGovernanceDeadItem{
				Kind: "alert", ID: row.ID, Term: row.Term, IncidentID: row.IncidentID, Identity: row.EvidenceHash,
				Attempts: row.AlertAttempts, LastError: row.AlertLastError, CreatedAt: row.CreatedAt,
			})
		}
		return items, nil
	default:
		return nil, fmt.Errorf("governance dead kind must be anchor or alert")
	}
}

func (r *TaskRepository) RequeueControlPlaneSIEMGovernanceDead(
	ctx context.Context,
	kind string,
	id uint64,
	operatorID, reason string,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceRemediation, error) {
	kind = strings.TrimSpace(strings.ToLower(kind))
	operatorID = strings.TrimSpace(operatorID)
	reason = strings.TrimSpace(reason)
	if id == 0 || operatorID == "" || reason == "" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, fmt.Errorf("governance remediation requires target id, operator and reason")
	}
	if kind != "anchor" && kind != "alert" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, fmt.Errorf("governance dead kind must be anchor or alert")
	}
	now = now.UTC().Truncate(time.Millisecond)
	remediation := domain.ControlPlaneSIEMGovernanceRemediation{
		Kind: kind, TargetID: id, Action: "requeue", OperatorID: operatorID, Reason: reason, State: "open", CreatedAt: now,
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updates := map[string]any{}
		if kind == "anchor" {
			var row domain.ControlPlaneSIEMReceiverTermHistory
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
				return err
			}
			if row.AnchorState != domain.ControlPlaneDeliveryDead {
				return fmt.Errorf("governance anchor %d is not dead", id)
			}
			remediation.PreviousState = row.AnchorState
			remediation.PreviousError = boundedGovernanceDeliveryError(errors.New(row.AnchorLastError))
			updates = map[string]any{
				"anchor_state": domain.ControlPlaneDeliveryPending, "anchor_next_at": now,
				"anchor_lease_token": "", "anchor_lease_until": nil, "anchor_last_error": "",
			}
			if err := tx.Model(&row).Updates(updates).Error; err != nil {
				return err
			}
		} else {
			var row domain.ControlPlaneSIEMIncidentEvidence
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
				return err
			}
			if row.AlertState != domain.ControlPlaneDeliveryDead {
				return fmt.Errorf("governance alert %d is not dead", id)
			}
			remediation.PreviousState = row.AlertState
			remediation.PreviousError = boundedGovernanceDeliveryError(errors.New(row.AlertLastError))
			updates = map[string]any{
				"alert_state": domain.ControlPlaneDeliveryPending, "alert_next_at": now,
				"alert_lease_token": "", "alert_lease_until": nil, "alert_last_error": "",
			}
			if err := tx.Model(&row).Updates(updates).Error; err != nil {
				return err
			}
		}
		return tx.Create(&remediation).Error
	})
	return remediation, err
}

func (r *TaskRepository) RequestControlPlaneSIEMGovernanceRemediation(
	ctx context.Context,
	kind string,
	id uint64,
	operatorID, reason, operationKey string,
	expiresAt time.Time,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error) {
	kind = strings.TrimSpace(strings.ToLower(kind))
	operatorID = strings.TrimSpace(operatorID)
	reason = strings.TrimSpace(reason)
	operationKey = strings.TrimSpace(operationKey)
	if id == 0 || operatorID == "" || reason == "" || operationKey == "" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation request requires target id, operator, reason and operation key")
	}
	if len(operationKey) > 128 {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation operation key exceeds 128 characters")
	}
	if kind != "anchor" && kind != "alert" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance dead kind must be anchor or alert")
	}
	now = now.UTC().Truncate(time.Millisecond)
	expiresAt = expiresAt.UTC().Truncate(time.Millisecond)
	if !expiresAt.After(now) {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation approval expiry must be in the future")
	}
	key := operationKey
	remediation := domain.ControlPlaneSIEMGovernanceRemediation{
		OperationKey: &key,
		Kind:         kind,
		TargetID:     id,
		Action:       "requeue",
		OperatorID:   operatorID,
		Reason:       reason,
		State:        "pending_approval",
		ExpiresAt:    &expiresAt,
		CreatedAt:    now,
	}
	idempotent := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing domain.ControlPlaneSIEMGovernanceRemediation
		lookupErr := tx.Where("operation_key = ?", operationKey).First(&existing).Error
		if lookupErr == nil {
			if existing.Kind != kind || existing.TargetID != id || existing.OperatorID != operatorID || existing.Reason != reason || existing.Action != "requeue" {
				return fmt.Errorf("governance remediation operation key %q conflicts with an existing request", operationKey)
			}
			remediation = existing
			idempotent = true
			return nil
		}
		if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			return lookupErr
		}

		if kind == "anchor" {
			var row domain.ControlPlaneSIEMReceiverTermHistory
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
				return err
			}
			if row.AnchorState != domain.ControlPlaneDeliveryDead {
				return fmt.Errorf("governance anchor %d is not dead", id)
			}
			remediation.PreviousState = row.AnchorState
			remediation.PreviousError = boundedGovernanceDeliveryError(errors.New(row.AnchorLastError))
			remediation.TargetIdentity = row.RecordHash
		} else {
			var row domain.ControlPlaneSIEMIncidentEvidence
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, id).Error; err != nil {
				return err
			}
			if row.AlertState != domain.ControlPlaneDeliveryDead {
				return fmt.Errorf("governance alert %d is not dead", id)
			}
			remediation.PreviousState = row.AlertState
			remediation.PreviousError = boundedGovernanceDeliveryError(errors.New(row.AlertLastError))
			remediation.TargetIdentity = row.EvidenceHash
		}

		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&remediation)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}
		if err := tx.Where("operation_key = ?", operationKey).First(&existing).Error; err != nil {
			return err
		}
		if existing.Kind != kind || existing.TargetID != id || existing.OperatorID != operatorID || existing.Reason != reason || existing.Action != "requeue" {
			return fmt.Errorf("governance remediation operation key %q conflicts with an existing request", operationKey)
		}
		remediation = existing
		idempotent = true
		return nil
	})
	return remediation, idempotent, err
}

func (r *TaskRepository) ApproveControlPlaneSIEMGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	approvedBy, note string,
	enforceTargetIdentity bool,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error) {
	approvedBy = strings.TrimSpace(approvedBy)
	note = strings.TrimSpace(note)
	if remediationID == 0 || approvedBy == "" || note == "" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation approval requires remediation id, reviewer and note")
	}
	now = now.UTC().Truncate(time.Millisecond)
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	idempotent := false
	var decisionErr error
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&remediation, remediationID).Error; err != nil {
			return err
		}
		if remediation.State != "pending_approval" {
			if remediation.ApprovedBy == approvedBy && remediation.ApprovalNote == note && (remediation.State == "open" || remediation.State == "resolved" || remediation.State == "failed") {
				idempotent = true
				return nil
			}
			return fmt.Errorf("governance remediation %d is not pending approval", remediationID)
		}
		if remediation.OperatorID == approvedBy {
			return fmt.Errorf("governance remediation requester and reviewer must be different operators")
		}
		if remediation.ExpiresAt != nil && !now.Before(*remediation.ExpiresAt) {
			result := "governance remediation approval window expired"
			if err := tx.Model(&remediation).Updates(map[string]any{
				"state": "expired", "result": result, "resolved_at": now,
			}).Error; err != nil {
				return err
			}
			remediation.State = "expired"
			remediation.Result = result
			remediation.ResolvedAt = &now
			decisionErr = fmt.Errorf("governance remediation %d approval expired", remediationID)
			return nil
		}

		if remediation.Kind == "anchor" {
			var row domain.ControlPlaneSIEMReceiverTermHistory
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, remediation.TargetID).Error; err != nil {
				return err
			}
			if row.AnchorState != domain.ControlPlaneDeliveryDead || (enforceTargetIdentity && remediation.TargetIdentity != "" && row.RecordHash != remediation.TargetIdentity) {
				result := boundedGovernanceDeliveryError(errors.New(fmt.Sprintf("stale governance anchor target state=%s identity=%s", row.AnchorState, row.RecordHash)))
				if err := tx.Model(&remediation).Updates(map[string]any{
					"state": "stale", "result": result, "resolved_at": now,
				}).Error; err != nil {
					return err
				}
				remediation.State = "stale"
				remediation.Result = result
				remediation.ResolvedAt = &now
				decisionErr = fmt.Errorf("governance remediation %d target changed after request", remediationID)
				return nil
			}
			if err := tx.Model(&row).Updates(map[string]any{
				"anchor_state": domain.ControlPlaneDeliveryPending, "anchor_next_at": now,
				"anchor_lease_token": "", "anchor_lease_until": nil, "anchor_last_error": "",
			}).Error; err != nil {
				return err
			}
		} else if remediation.Kind == "alert" {
			var row domain.ControlPlaneSIEMIncidentEvidence
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, remediation.TargetID).Error; err != nil {
				return err
			}
			if row.AlertState != domain.ControlPlaneDeliveryDead || (enforceTargetIdentity && remediation.TargetIdentity != "" && row.EvidenceHash != remediation.TargetIdentity) {
				result := boundedGovernanceDeliveryError(errors.New(fmt.Sprintf("stale governance alert target state=%s identity=%s", row.AlertState, row.EvidenceHash)))
				if err := tx.Model(&remediation).Updates(map[string]any{
					"state": "stale", "result": result, "resolved_at": now,
				}).Error; err != nil {
					return err
				}
				remediation.State = "stale"
				remediation.Result = result
				remediation.ResolvedAt = &now
				decisionErr = fmt.Errorf("governance remediation %d target changed after request", remediationID)
				return nil
			}
			if err := tx.Model(&row).Updates(map[string]any{
				"alert_state": domain.ControlPlaneDeliveryPending, "alert_next_at": now,
				"alert_lease_token": "", "alert_lease_until": nil, "alert_last_error": "",
			}).Error; err != nil {
				return err
			}
		} else {
			return fmt.Errorf("governance remediation %d has unsupported kind %q", remediationID, remediation.Kind)
		}

		if err := tx.Model(&remediation).Updates(map[string]any{
			"approved_by":   approvedBy,
			"approval_note": note,
			"approved_at":   now,
			"state":         "open",
		}).Error; err != nil {
			return err
		}
		remediation.ApprovedBy = approvedBy
		remediation.ApprovalNote = note
		remediation.ApprovedAt = &now
		remediation.State = "open"
		return nil
	})
	if err == nil && decisionErr != nil {
		return remediation, false, decisionErr
	}
	return remediation, idempotent, err
}

func (r *TaskRepository) RejectControlPlaneSIEMGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	rejectedBy, note string,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error) {
	rejectedBy = strings.TrimSpace(rejectedBy)
	note = strings.TrimSpace(note)
	if remediationID == 0 || rejectedBy == "" || note == "" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation rejection requires remediation id, reviewer and note")
	}
	now = now.UTC().Truncate(time.Millisecond)
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	idempotent := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&remediation, remediationID).Error; err != nil {
			return err
		}
		if remediation.State == "rejected" && remediation.RejectedBy == rejectedBy && remediation.RejectionNote == note {
			idempotent = true
			return nil
		}
		if remediation.State != "pending_approval" {
			return fmt.Errorf("governance remediation %d is not pending approval", remediationID)
		}
		if remediation.OperatorID == rejectedBy {
			return fmt.Errorf("governance remediation requester and reviewer must be different operators")
		}
		if remediation.ExpiresAt != nil && !now.Before(*remediation.ExpiresAt) {
			return fmt.Errorf("governance remediation %d approval expired", remediationID)
		}
		result := "governance remediation rejected by reviewer"
		if err := tx.Model(&remediation).Updates(map[string]any{
			"rejected_by": rejectedBy, "rejection_note": note, "rejected_at": now,
			"state": "rejected", "result": result, "resolved_at": now,
		}).Error; err != nil {
			return err
		}
		remediation.RejectedBy = rejectedBy
		remediation.RejectionNote = note
		remediation.RejectedAt = &now
		remediation.State = "rejected"
		remediation.Result = result
		remediation.ResolvedAt = &now
		return nil
	})
	return remediation, idempotent, err
}

func (r *TaskRepository) CancelControlPlaneSIEMGovernanceRemediation(
	ctx context.Context,
	remediationID uint64,
	cancelledBy, note string,
	now time.Time,
) (domain.ControlPlaneSIEMGovernanceRemediation, bool, error) {
	cancelledBy = strings.TrimSpace(cancelledBy)
	note = strings.TrimSpace(note)
	if remediationID == 0 || cancelledBy == "" || note == "" {
		return domain.ControlPlaneSIEMGovernanceRemediation{}, false, fmt.Errorf("governance remediation cancellation requires remediation id, requester and note")
	}
	now = now.UTC().Truncate(time.Millisecond)
	var remediation domain.ControlPlaneSIEMGovernanceRemediation
	idempotent := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&remediation, remediationID).Error; err != nil {
			return err
		}
		if remediation.State == "cancelled" && remediation.CancelledBy == cancelledBy && remediation.CancelNote == note {
			idempotent = true
			return nil
		}
		if remediation.State != "pending_approval" {
			return fmt.Errorf("governance remediation %d is not pending approval", remediationID)
		}
		if remediation.OperatorID != cancelledBy {
			return fmt.Errorf("only governance remediation requester %s may cancel request", remediation.OperatorID)
		}
		result := "governance remediation cancelled by requester"
		if err := tx.Model(&remediation).Updates(map[string]any{
			"cancelled_by": cancelledBy, "cancel_note": note, "cancelled_at": now,
			"state": "cancelled", "result": result, "resolved_at": now,
		}).Error; err != nil {
			return err
		}
		remediation.CancelledBy = cancelledBy
		remediation.CancelNote = note
		remediation.CancelledAt = &now
		remediation.State = "cancelled"
		remediation.Result = result
		remediation.ResolvedAt = &now
		return nil
	})
	return remediation, idempotent, err
}

func (r *TaskRepository) ExpireControlPlaneSIEMGovernanceRemediations(ctx context.Context, now time.Time) (int64, error) {
	now = now.UTC().Truncate(time.Millisecond)
	result := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceRemediation{}).
		Where("state = ? AND expires_at IS NOT NULL AND expires_at <= ?", "pending_approval", now).
		Updates(map[string]any{
			"state": "expired", "result": "governance remediation approval window expired", "resolved_at": now,
		})
	return result.RowsAffected, result.Error
}

func (r *TaskRepository) SettleControlPlaneSIEMGovernanceRemediations(
	ctx context.Context,
	kind string,
	targetID uint64,
	state, result string,
	now time.Time,
) (bool, error) {
	kind = strings.TrimSpace(strings.ToLower(kind))
	state = strings.TrimSpace(strings.ToLower(state))
	if (kind != "anchor" && kind != "alert") || targetID == 0 || (state != "resolved" && state != "failed") {
		return false, fmt.Errorf("invalid governance remediation settlement")
	}
	now = now.UTC().Truncate(time.Millisecond)
	resultUpdate := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMGovernanceRemediation{}).
		Where("kind = ? AND target_id = ? AND state = ?", kind, targetID, "open").
		Updates(map[string]any{
			"state":       state,
			"result":      boundedGovernanceDeliveryError(errors.New(result)),
			"resolved_at": now,
		})
	return resultUpdate.RowsAffected > 0, resultUpdate.Error
}

func (r *TaskRepository) ListControlPlaneSIEMGovernanceRemediations(
	ctx context.Context,
	limit int,
) ([]domain.ControlPlaneSIEMGovernanceRemediation, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []domain.ControlPlaneSIEMGovernanceRemediation
	err := r.db.WithContext(ctx).Order("id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

func (r *TaskRepository) ClaimControlPlaneSIEMTermAnchorDeliveries(
	ctx context.Context,
	now, leaseUntil time.Time,
	leaseToken string,
	limit int,
) ([]domain.ControlPlaneSIEMReceiverTermHistory, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	leaseToken = strings.TrimSpace(leaseToken)
	if leaseToken == "" {
		return nil, fmt.Errorf("governance term-anchor lease token is required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	leaseUntil = leaseUntil.UTC().Truncate(time.Millisecond)
	var rows []domain.ControlPlaneSIEMReceiverTermHistory
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("((anchor_state IN ? AND (anchor_next_at IS NULL OR anchor_next_at <= ?)) OR (anchor_state = ? AND anchor_lease_until < ?))",
				[]string{"", domain.ControlPlaneDeliveryPending}, now,
				domain.ControlPlaneDeliveryDelivering, now).
			Order("term ASC").
			Limit(limit).
			Find(&rows)
		if result.Error != nil || len(rows) == 0 {
			return result.Error
		}
		ids := make([]uint64, 0, len(rows))
		for i := range rows {
			ids = append(ids, rows[i].ID)
			rows[i].AnchorState = domain.ControlPlaneDeliveryDelivering
			rows[i].AnchorAttempts++
			rows[i].AnchorLeaseToken = leaseToken
			rows[i].AnchorLeaseUntil = &leaseUntil
		}
		return tx.Model(&domain.ControlPlaneSIEMReceiverTermHistory{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"anchor_state":       domain.ControlPlaneDeliveryDelivering,
				"anchor_attempts":    gorm.Expr("anchor_attempts + 1"),
				"anchor_lease_token": leaseToken,
				"anchor_lease_until": leaseUntil,
			}).Error
	})
	return rows, err
}

func (r *TaskRepository) MarkControlPlaneSIEMTermAnchorDelivered(ctx context.Context, term uint64, deliveredAt time.Time) error {
	deliveredAt = deliveredAt.UTC().Truncate(time.Millisecond)
	return r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMReceiverTermHistory{}).
		Where("term = ?", term).
		Updates(map[string]any{
			"anchor_state":       domain.ControlPlaneDeliveryDelivered,
			"anchor_next_at":     deliveredAt,
			"anchor_lease_token": "",
			"anchor_lease_until": nil,
			"anchor_last_error":  "",
		}).Error
}

func (r *TaskRepository) FailControlPlaneSIEMTermAnchorDelivery(
	ctx context.Context,
	delivery domain.ControlPlaneSIEMReceiverTermHistory,
	leaseToken string,
	now, retryAt time.Time,
	cause error,
	maxAttempts int,
) (bool, error) {
	state := domain.ControlPlaneDeliveryPending
	dead := maxAttempts > 0 && delivery.AnchorAttempts >= maxAttempts
	if dead {
		state = domain.ControlPlaneDeliveryDead
	}
	result := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMReceiverTermHistory{}).
		Where("id = ? AND anchor_state = ? AND anchor_lease_token = ?", delivery.ID, domain.ControlPlaneDeliveryDelivering, leaseToken).
		Updates(map[string]any{
			"anchor_state":       state,
			"anchor_next_at":     retryAt.UTC().Truncate(time.Millisecond),
			"anchor_lease_token": "",
			"anchor_lease_until": nil,
			"anchor_last_error":  boundedGovernanceDeliveryError(cause),
		})
	return dead, result.Error
}

func (r *TaskRepository) ClaimControlPlaneSIEMIncidentAlertDeliveries(
	ctx context.Context,
	now, leaseUntil time.Time,
	leaseToken string,
	limit int,
) ([]domain.ControlPlaneSIEMIncidentEvidence, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	leaseToken = strings.TrimSpace(leaseToken)
	if leaseToken == "" {
		return nil, fmt.Errorf("governance incident-alert lease token is required")
	}
	now = now.UTC().Truncate(time.Millisecond)
	leaseUntil = leaseUntil.UTC().Truncate(time.Millisecond)
	var rows []domain.ControlPlaneSIEMIncidentEvidence
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("((alert_state IN ? AND (alert_next_at IS NULL OR alert_next_at <= ?)) OR (alert_state = ? AND alert_lease_until < ?))",
				[]string{"", domain.ControlPlaneDeliveryPending}, now,
				domain.ControlPlaneDeliveryDelivering, now).
			Order("id ASC").
			Limit(limit).
			Find(&rows)
		if result.Error != nil || len(rows) == 0 {
			return result.Error
		}
		ids := make([]uint64, 0, len(rows))
		for i := range rows {
			ids = append(ids, rows[i].ID)
			rows[i].AlertState = domain.ControlPlaneDeliveryDelivering
			rows[i].AlertAttempts++
			rows[i].AlertLeaseToken = leaseToken
			rows[i].AlertLeaseUntil = &leaseUntil
		}
		return tx.Model(&domain.ControlPlaneSIEMIncidentEvidence{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"alert_state":       domain.ControlPlaneDeliveryDelivering,
				"alert_attempts":    gorm.Expr("alert_attempts + 1"),
				"alert_lease_token": leaseToken,
				"alert_lease_until": leaseUntil,
			}).Error
	})
	return rows, err
}

func (r *TaskRepository) MarkControlPlaneSIEMIncidentAlertDelivered(ctx context.Context, id uint64, deliveredAt time.Time) error {
	deliveredAt = deliveredAt.UTC().Truncate(time.Millisecond)
	return r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMIncidentEvidence{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"alert_state":       domain.ControlPlaneDeliveryDelivered,
			"alert_next_at":     deliveredAt,
			"alert_lease_token": "",
			"alert_lease_until": nil,
			"alert_last_error":  "",
			"alerted_at":        deliveredAt,
		}).Error
}

func (r *TaskRepository) FailControlPlaneSIEMIncidentAlertDelivery(
	ctx context.Context,
	delivery domain.ControlPlaneSIEMIncidentEvidence,
	leaseToken string,
	now, retryAt time.Time,
	cause error,
	maxAttempts int,
) (bool, error) {
	state := domain.ControlPlaneDeliveryPending
	dead := maxAttempts > 0 && delivery.AlertAttempts >= maxAttempts
	if dead {
		state = domain.ControlPlaneDeliveryDead
	}
	result := r.db.WithContext(ctx).Model(&domain.ControlPlaneSIEMIncidentEvidence{}).
		Where("id = ? AND alert_state = ? AND alert_lease_token = ?", delivery.ID, domain.ControlPlaneDeliveryDelivering, leaseToken).
		Updates(map[string]any{
			"alert_state":       state,
			"alert_next_at":     retryAt.UTC().Truncate(time.Millisecond),
			"alert_lease_token": "",
			"alert_lease_until": nil,
			"alert_last_error":  boundedGovernanceDeliveryError(cause),
		})
	return dead, result.Error
}

func boundedGovernanceDeliveryError(cause error) string {
	if cause == nil {
		return ""
	}
	value := []rune(strings.TrimSpace(cause.Error()))
	if len(value) > 1000 {
		value = value[:1000]
	}
	return string(value)
}
