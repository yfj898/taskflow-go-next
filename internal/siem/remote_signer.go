package siem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type RemoteSignerConfig struct {
	Endpoint    string
	BearerToken string
	KeyID       string
	Algorithm   string
	Timeout     time.Duration
}

type RemoteCheckpointSigner struct {
	config RemoteSignerConfig
	client *http.Client
}

type remoteSignRequest struct {
	Version      string `json:"version"`
	KeyID        string `json:"key_id"`
	Algorithm    string `json:"algorithm"`
	DigestSHA256 string `json:"digest_sha256"`
}

type remoteSignResponse struct {
	Signature string `json:"signature"`
	KeyID     string `json:"key_id,omitempty"`
	Algorithm string `json:"algorithm,omitempty"`
}

func NewRemoteCheckpointSigner(config RemoteSignerConfig) (*RemoteCheckpointSigner, error) {
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	config.KeyID = strings.TrimSpace(config.KeyID)
	config.Algorithm = strings.TrimSpace(config.Algorithm)
	if config.Endpoint == "" {
		return nil, fmt.Errorf("remote SIEM signer endpoint is empty")
	}
	if !strings.HasPrefix(config.Endpoint, "http://") && !strings.HasPrefix(config.Endpoint, "https://") {
		return nil, fmt.Errorf("remote SIEM signer endpoint must use http or https")
	}
	if config.KeyID == "" {
		return nil, fmt.Errorf("remote SIEM signer key id is empty")
	}
	if config.Algorithm == "" {
		config.Algorithm = "hmac-sha256-digest"
	}
	if config.Timeout <= 0 {
		config.Timeout = 3 * time.Second
	}
	return &RemoteCheckpointSigner{
		config: config,
		client: &http.Client{Timeout: config.Timeout},
	}, nil
}

func (s *RemoteCheckpointSigner) KeyID() string { return s.config.KeyID }

func (s *RemoteCheckpointSigner) Algorithm() string { return s.config.Algorithm }

func (s *RemoteCheckpointSigner) Sign(payload []byte) (string, error) {
	digest := sha256.Sum256(payload)
	requestBody, err := json.Marshal(remoteSignRequest{
		Version:      "taskflow-remote-signer-v1",
		KeyID:        s.config.KeyID,
		Algorithm:    s.config.Algorithm,
		DigestSHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.config.Endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return "", fmt.Errorf("build remote signer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(s.config.BearerToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("remote signer request: %w", err)
	}
	defer resp.Body.Close()
	limited, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return "", fmt.Errorf("read remote signer response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("remote signer returned HTTP %d", resp.StatusCode)
	}
	var result remoteSignResponse
	if err := json.Unmarshal(limited, &result); err != nil {
		return "", fmt.Errorf("decode remote signer response: %w", err)
	}
	result.Signature = strings.TrimSpace(result.Signature)
	if result.Signature == "" {
		return "", fmt.Errorf("remote signer returned empty signature")
	}
	if keyID := strings.TrimSpace(result.KeyID); keyID != "" && keyID != s.config.KeyID {
		return "", fmt.Errorf("remote signer key id mismatch: got %q", keyID)
	}
	if algorithm := strings.TrimSpace(result.Algorithm); algorithm != "" && algorithm != s.config.Algorithm {
		return "", fmt.Errorf("remote signer algorithm mismatch: got %q", algorithm)
	}
	return result.Signature, nil
}
