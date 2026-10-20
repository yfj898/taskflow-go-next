package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func ComputeControlPlaneSIEMReceiverTermHistoryHash(entry ControlPlaneSIEMReceiverTermHistory) (string, error) {
	if entry.HashVersion >= 2 {
		canonical := struct {
			HashVersion      uint   `json:"hash_version"`
			Term             uint64 `json:"term"`
			PreviousTerm     uint64 `json:"previous_term"`
			PreviousReceiver string `json:"previous_receiver"`
			ActiveReceiver   string `json:"active_receiver"`
			Action           string `json:"action"`
			IntentID         uint64 `json:"intent_id"`
			RequestedBy      string `json:"requested_by"`
			ApprovedBy       string `json:"approved_by"`
			Reason           string `json:"reason"`
			RPOSequences     uint64 `json:"rpo_sequences"`
			Emergency        bool   `json:"emergency"`
			IncidentID       string `json:"incident_id"`
			PreviousHash     string `json:"previous_hash"`
		}{
			HashVersion:      entry.HashVersion,
			Term:             entry.Term,
			PreviousTerm:     entry.PreviousTerm,
			PreviousReceiver: entry.PreviousReceiver,
			ActiveReceiver:   entry.ActiveReceiver,
			Action:           entry.Action,
			IntentID:         entry.IntentID,
			RequestedBy:      entry.RequestedBy,
			ApprovedBy:       entry.ApprovedBy,
			Reason:           entry.Reason,
			RPOSequences:     entry.RPOSequences,
			Emergency:        entry.Emergency,
			IncidentID:       entry.IncidentID,
			PreviousHash:     entry.PreviousHash,
		}
		body, err := json.Marshal(canonical)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(body)
		return hex.EncodeToString(digest[:]), nil
	}
	canonical := struct {
		Term             uint64 `json:"term"`
		PreviousTerm     uint64 `json:"previous_term"`
		PreviousReceiver string `json:"previous_receiver"`
		ActiveReceiver   string `json:"active_receiver"`
		Action           string `json:"action"`
		IntentID         uint64 `json:"intent_id"`
		RequestedBy      string `json:"requested_by"`
		ApprovedBy       string `json:"approved_by"`
		Reason           string `json:"reason"`
		RPOSequences     uint64 `json:"rpo_sequences"`
		PreviousHash     string `json:"previous_hash"`
	}{
		Term:             entry.Term,
		PreviousTerm:     entry.PreviousTerm,
		PreviousReceiver: entry.PreviousReceiver,
		ActiveReceiver:   entry.ActiveReceiver,
		Action:           entry.Action,
		IntentID:         entry.IntentID,
		RequestedBy:      entry.RequestedBy,
		ApprovedBy:       entry.ApprovedBy,
		Reason:           entry.Reason,
		RPOSequences:     entry.RPOSequences,
		PreviousHash:     entry.PreviousHash,
	}
	body, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}
