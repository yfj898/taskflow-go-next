package siem

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	receiverCheckpointSignatureHeader  = "X-TaskFlow-Checkpoint-Signature"
	receiverCheckpointKeyIDHeader      = "X-TaskFlow-Checkpoint-Key-ID"
	receiverCheckpointReceiverIDHeader = "X-TaskFlow-Receiver-ID"
	receiverCheckpointGenerationHeader = "X-TaskFlow-Receiver-Generation"
)

type ReceiverCheckpointSignatureVerifier struct {
	KeyID string
	Key   []byte
}

func NewReceiverCheckpointSignatureVerifier(keyID, key string) (*ReceiverCheckpointSignatureVerifier, error) {
	keyID = strings.TrimSpace(keyID)
	key = strings.TrimSpace(key)
	if keyID == "" || key == "" {
		return nil, errors.New("receiver checkpoint HMAC key id and key are required")
	}
	return &ReceiverCheckpointSignatureVerifier{KeyID: keyID, Key: []byte(key)}, nil
}

func (v *ReceiverCheckpointSignatureVerifier) Verify(body []byte, headers http.Header) (string, uint64, error) {
	if v == nil {
		return "", 0, nil
	}
	keyID := strings.TrimSpace(headers.Get(receiverCheckpointKeyIDHeader))
	if keyID != v.KeyID {
		return "", 0, fmt.Errorf("receiver checkpoint signing key mismatch: got=%q want=%q", keyID, v.KeyID)
	}
	signature := strings.TrimSpace(headers.Get(receiverCheckpointSignatureHeader))
	if !strings.HasPrefix(signature, "sha256=") {
		return "", 0, errors.New("receiver checkpoint signature is missing or malformed")
	}
	expectedMAC := hmac.New(sha256.New, v.Key)
	_, _ = expectedMAC.Write(body)
	expected := expectedMAC.Sum(nil)
	actual, err := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	if err != nil || !hmac.Equal(expected, actual) {
		return "", 0, errors.New("receiver checkpoint HMAC verification failed")
	}
	receiverID := strings.TrimSpace(headers.Get(receiverCheckpointReceiverIDHeader))
	if receiverID == "" {
		return "", 0, errors.New("receiver checkpoint receiver id is empty")
	}
	generationText := strings.TrimSpace(headers.Get(receiverCheckpointGenerationHeader))
	generation, err := strconv.ParseUint(generationText, 10, 64)
	if err != nil || generation == 0 {
		return "", 0, errors.New("receiver checkpoint generation is invalid")
	}
	return receiverID, generation, nil
}

func fetchReceiverCheckpoint(
	ctx context.Context,
	client *http.Client,
	endpoint, bearerToken string,
	after uint64,
	limit int,
	verifier *ReceiverCheckpointSignatureVerifier,
	expectedReceiver string,
) (receiverCheckpointResponse, error) {
	var result receiverCheckpointResponse
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return result, errors.New("SIEM checkpoint endpoint must be an absolute http(s) URL")
	}
	query := u.Query()
	query.Set("after_sequence", strconv.FormatUint(after, 10))
	query.Set("limit", strconv.Itoa(limit))
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return result, fmt.Errorf("build SIEM checkpoint request: %w", err)
	}
	if token := strings.TrimSpace(bearerToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("fetch SIEM checkpoint: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if readErr != nil {
		return result, fmt.Errorf("read SIEM checkpoint response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("SIEM checkpoint returned HTTP %d", resp.StatusCode)
	}
	headerReceiver := ""
	headerGeneration := uint64(0)
	if verifier != nil {
		headerReceiver, headerGeneration, err = verifier.Verify(body, resp.Header)
		if err != nil {
			return result, fmt.Errorf("verify SIEM checkpoint signature: %w", err)
		}
		if expected := strings.TrimSpace(expectedReceiver); expected != "" && headerReceiver != expected {
			return result, fmt.Errorf("SIEM checkpoint receiver mismatch: got=%q want=%q", headerReceiver, expected)
		}
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, fmt.Errorf("decode SIEM checkpoint response: %w", err)
	}
	if verifier != nil {
		if result.ReceiverID != headerReceiver {
			return result, errors.New("SIEM checkpoint body/header receiver id mismatch")
		}
		if result.ReceiverGeneration != headerGeneration {
			return result, errors.New("SIEM checkpoint body/header generation mismatch")
		}
	}
	return result, nil
}
