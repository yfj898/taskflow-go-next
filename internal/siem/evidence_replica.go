package siem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	archivepkg "github.com/yfj898/taskflow-go/internal/archive"
)

type EvidenceReplica interface {
	Replicate(context.Context, string, []byte) (int, error)
}

type ReceiptEvidenceVerifier interface {
	VerifyReceiptEvidence(context.Context, string, deliveryReceiptProof, int) (int, string, error)
}

type ReceiverReceiptEvidenceWriter interface {
	ReplicateReceiverReceipt(context.Context, uint64, string, deliveryReceiptProof) (int, error)
}

type receiverReceiptEvidence struct {
	Version            string               `json:"version"`
	ReceiverReceipt    deliveryReceiptProof `json:"receiver_receipt"`
	ReceiptProofSHA256 string               `json:"receipt_proof_sha256"`
}

type MultiSinkEvidenceReplica struct {
	sinks     []archivepkg.Sink
	minCopies int
}

func NewMultiSinkEvidenceReplica(minCopies int, sinks ...archivepkg.Sink) (*MultiSinkEvidenceReplica, error) {
	filtered := make([]archivepkg.Sink, 0, len(sinks))
	for _, sink := range sinks {
		if sink != nil {
			filtered = append(filtered, sink)
		}
	}
	if len(filtered) == 0 {
		return nil, errors.New("SIEM evidence replica has no sinks")
	}
	if minCopies <= 0 {
		minCopies = len(filtered)
	}
	if minCopies > len(filtered) {
		return nil, fmt.Errorf("SIEM evidence replica min copies %d exceeds sinks %d", minCopies, len(filtered))
	}
	return &MultiSinkEvidenceReplica{sinks: filtered, minCopies: minCopies}, nil
}

func (r *MultiSinkEvidenceReplica) Replicate(ctx context.Context, key string, data []byte) (int, error) {
	want := sha256.Sum256(data)
	copies := 0
	var failures []error
	for _, sink := range r.sinks {
		existing, err := sink.Get(ctx, key)
		if err == nil {
			got := sha256.Sum256(existing)
			if got != want {
				failures = append(failures, fmt.Errorf("SIEM evidence sink %s contains conflicting object %s", sink.Name(), key))
				continue
			}
			copies++
			continue
		}
		if !errors.Is(err, archivepkg.ErrObjectNotFound) {
			failures = append(failures, fmt.Errorf("read SIEM evidence sink %s: %w", sink.Name(), err))
			continue
		}
		if err := sink.Put(ctx, key, data); err != nil {
			failures = append(failures, fmt.Errorf("write SIEM evidence sink %s: %w", sink.Name(), err))
			continue
		}
		verified, err := sink.Get(ctx, key)
		if err != nil {
			failures = append(failures, fmt.Errorf("verify SIEM evidence sink %s: %w", sink.Name(), err))
			continue
		}
		got := sha256.Sum256(verified)
		if got != want {
			failures = append(failures, fmt.Errorf("SIEM evidence sink %s hash mismatch: got=%s want=%s", sink.Name(), hex.EncodeToString(got[:]), hex.EncodeToString(want[:])))
			continue
		}
		copies++
	}
	if copies < r.minCopies {
		return copies, fmt.Errorf("SIEM evidence durability copies=%d required=%d: %w", copies, r.minCopies, errors.Join(failures...))
	}
	return copies, nil
}

func (r *MultiSinkEvidenceReplica) ReplicateReceiverReceipt(ctx context.Context, sequence uint64, idempotencyKey string, proof deliveryReceiptProof) (int, error) {
	proofBody, err := json.Marshal(proof)
	if err != nil {
		return 0, fmt.Errorf("marshal SIEM receiver anti-entropy proof: %w", err)
	}
	digest := sha256.Sum256(proofBody)
	body, err := json.Marshal(receiverReceiptEvidence{
		Version:            "taskflow-control-plane-siem-receiver-receipt-v1",
		ReceiverReceipt:    proof,
		ReceiptProofSHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return 0, fmt.Errorf("marshal SIEM receiver anti-entropy evidence: %w", err)
	}
	return r.Replicate(ctx, deliveryReceiverReceiptEvidenceObjectKey(sequence, idempotencyKey, proof.ReceiverID), body)
}

func (r *MultiSinkEvidenceReplica) VerifyReceiptEvidence(ctx context.Context, key string, expected deliveryReceiptProof, minCopies int) (int, string, error) {
	if minCopies <= 0 {
		minCopies = 1
	}
	if minCopies > len(r.sinks) {
		return 0, "", fmt.Errorf("SIEM reconciliation evidence copies=%d exceeds sinks=%d", minCopies, len(r.sinks))
	}
	validCopies := 0
	firstSource := ""
	var failures []error
	for _, sink := range r.sinks {
		data, err := sink.Get(ctx, key)
		if err != nil {
			failures = append(failures, fmt.Errorf("read SIEM reconciliation evidence sink %s: %w", sink.Name(), err))
			continue
		}
		var evidence deliveryEvidence
		if err := json.Unmarshal(data, &evidence); err != nil {
			failures = append(failures, fmt.Errorf("decode SIEM reconciliation evidence sink %s: %w", sink.Name(), err))
			continue
		}
		if evidence.Version != "taskflow-control-plane-siem-evidence-v1" ||
			evidence.IdempotencyKey != expected.IdempotencyKey ||
			evidence.FirstSequence != expected.FirstSequence ||
			evidence.LastSequence != expected.LastSequence ||
			!stringsEqualFold(evidence.PayloadSHA256, expected.PayloadSHA256) {
			failures = append(failures, fmt.Errorf("SIEM reconciliation evidence sink %s does not match receiver receipt", sink.Name()))
			continue
		}
		matchedReceiver, proofSetValid := evidenceContainsReceiverProof(evidence, expected)
		if !matchedReceiver && strings.TrimSpace(expected.ReceiverID) != "" {
			supplement, supplementErr := sink.Get(ctx, deliveryReceiverReceiptEvidenceObjectKey(expected.FirstSequence, expected.IdempotencyKey, expected.ReceiverID))
			if supplementErr == nil {
				matchedReceiver = validReceiverReceiptSupplement(supplement, expected)
			}
		}
		if !matchedReceiver || !proofSetValid {
			failures = append(failures, fmt.Errorf("SIEM reconciliation evidence sink %s has no valid matching receiver receipt proof", sink.Name()))
			continue
		}
		validCopies++
		if firstSource == "" {
			firstSource = sink.Name()
		}
	}
	if validCopies < minCopies {
		return validCopies, firstSource, fmt.Errorf("SIEM reconciliation evidence copies=%d required=%d: %w", validCopies, minCopies, errors.Join(failures...))
	}
	return validCopies, firstSource, nil
}

func evidenceContainsReceiverProof(evidence deliveryEvidence, expected deliveryReceiptProof) (bool, bool) {
	proofSetValid := true
	matched := false
	if evidence.ReceiverReceipt == expected {
		proofBytes, err := json.Marshal(evidence.ReceiverReceipt)
		if err == nil {
			digest := sha256.Sum256(proofBytes)
			matched = stringsEqualFold(evidence.ReceiptProofSHA256, hex.EncodeToString(digest[:]))
		}
	}
	if len(evidence.ReceiverReceipts) > 0 {
		proofSetBytes, err := json.Marshal(evidence.ReceiverReceipts)
		if err != nil {
			return false, false
		}
		proofSetDigest := sha256.Sum256(proofSetBytes)
		proofSetValid = stringsEqualFold(evidence.ReceiverReceiptsSHA256, hex.EncodeToString(proofSetDigest[:]))
		for _, proof := range evidence.ReceiverReceipts {
			if proof == expected {
				matched = true
				break
			}
		}
	}
	return matched, proofSetValid
}

func validReceiverReceiptSupplement(data []byte, expected deliveryReceiptProof) bool {
	var evidence receiverReceiptEvidence
	if err := json.Unmarshal(data, &evidence); err != nil || evidence.Version != "taskflow-control-plane-siem-receiver-receipt-v1" || evidence.ReceiverReceipt != expected {
		return false
	}
	proofBody, err := json.Marshal(evidence.ReceiverReceipt)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(proofBody)
	return stringsEqualFold(evidence.ReceiptProofSHA256, hex.EncodeToString(digest[:]))
}

func deliveryReceiverReceiptEvidenceObjectKey(sequence uint64, idempotencyKey, receiverID string) string {
	receiverDigest := sha256.Sum256([]byte(strings.TrimSpace(receiverID)))
	return fmt.Sprintf("control-plane-audit-receipts/%020d-%s-%s.json", sequence, idempotencyKey, hex.EncodeToString(receiverDigest[:8]))
}

func stringsEqualFold(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}
