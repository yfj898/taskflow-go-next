package siem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type VaultTransitSignerConfig struct {
	Endpoint   string
	Token      string
	Namespace  string
	Mount      string
	KeyName    string
	KeyID      string
	KeyVersion int
	Timeout    time.Duration
}

type VaultTransitSigner struct {
	config VaultTransitSignerConfig
	client *http.Client
}

type vaultTransitSignRequest struct {
	Input              string `json:"input"`
	Prehashed          bool   `json:"prehashed"`
	SignatureAlgorithm string `json:"signature_algorithm"`
	KeyVersion         int    `json:"key_version,omitempty"`
}

type vaultTransitSignResponse struct {
	Data struct {
		Signature  string `json:"signature"`
		KeyVersion int    `json:"key_version"`
	} `json:"data"`
}

func NewVaultTransitSigner(config VaultTransitSignerConfig) (*VaultTransitSigner, error) {
	config.Endpoint = strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	config.Token = strings.TrimSpace(config.Token)
	config.Namespace = strings.TrimSpace(config.Namespace)
	config.Mount = strings.Trim(strings.TrimSpace(config.Mount), "/")
	config.KeyName = strings.TrimSpace(config.KeyName)
	config.KeyID = strings.TrimSpace(config.KeyID)
	if config.Endpoint == "" {
		return nil, errors.New("Vault Transit endpoint is empty")
	}
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("Vault Transit endpoint must be an absolute http(s) URL")
	}
	if config.Token == "" {
		return nil, errors.New("Vault Transit token is empty")
	}
	if config.Mount == "" {
		config.Mount = "transit"
	}
	if strings.Contains(config.Mount, "..") || strings.Contains(config.Mount, "//") {
		return nil, errors.New("Vault Transit mount is invalid")
	}
	if config.KeyName == "" || strings.Contains(config.KeyName, "/") || strings.Contains(config.KeyName, "..") {
		return nil, errors.New("Vault Transit key name is invalid")
	}
	if config.KeyID == "" {
		config.KeyID = "vault:" + config.Mount + ":" + config.KeyName
	}
	if config.KeyVersion < 0 {
		return nil, errors.New("Vault Transit key version must be non-negative")
	}
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}
	return &VaultTransitSigner{config: config, client: &http.Client{Timeout: config.Timeout}}, nil
}

func (s *VaultTransitSigner) KeyID() string { return s.config.KeyID }

func (s *VaultTransitSigner) Algorithm() string {
	return "vault-transit-rsa-pkcs1v15-sha2-256-prehashed"
}

func (s *VaultTransitSigner) Sign(payload []byte) (string, error) {
	digest := sha256.Sum256(payload)
	requestBody, err := json.Marshal(vaultTransitSignRequest{
		Input:              base64.StdEncoding.EncodeToString(digest[:]),
		Prehashed:          true,
		SignatureAlgorithm: "pkcs1v15",
		KeyVersion:         s.config.KeyVersion,
	})
	if err != nil {
		return "", err
	}
	endpoint := s.config.Endpoint + "/v1/" + s.config.Mount + "/sign/" + url.PathEscape(s.config.KeyName) + "/sha2-256"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return "", fmt.Errorf("build Vault Transit request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vault-Token", s.config.Token)
	if s.config.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", s.config.Namespace)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Vault Transit sign request: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return "", fmt.Errorf("read Vault Transit response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("Vault Transit sign returned HTTP %d", resp.StatusCode)
	}
	var result vaultTransitSignResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode Vault Transit response: %w", err)
	}
	signature := strings.TrimSpace(result.Data.Signature)
	if !strings.HasPrefix(signature, "vault:v") || len(strings.Split(signature, ":")) != 3 {
		return "", errors.New("Vault Transit returned invalid signature")
	}
	if s.config.KeyVersion > 0 && result.Data.KeyVersion != 0 && result.Data.KeyVersion != s.config.KeyVersion {
		return "", fmt.Errorf("Vault Transit key version mismatch: got %d want %d", result.Data.KeyVersion, s.config.KeyVersion)
	}
	return signature, nil
}
