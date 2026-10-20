package siem

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRemoteCheckpointSignerSendsDigestOnly(t *testing.T) {
	const secret = "phase26-kms-secret"
	const token = "phase26-kms-token"
	payload := []byte(`{"sensitive_audit":"must-not-reach-signer"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
		var in remoteSignRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		if in.KeyID != "kms-key-v2" || in.Algorithm != "hmac-sha256-digest" {
			t.Fatalf("unexpected remote signing request: %+v", in)
		}
		digest := sha256.Sum256(payload)
		if in.DigestSHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("digest = %q", in.DigestSHA256)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(digest[:])
		_ = json.NewEncoder(w).Encode(remoteSignResponse{
			Signature: "sha256=" + hex.EncodeToString(mac.Sum(nil)),
			KeyID:     "kms-key-v2",
			Algorithm: "hmac-sha256-digest",
		})
	}))
	defer server.Close()

	signer, err := NewRemoteCheckpointSigner(RemoteSignerConfig{
		Endpoint:    server.URL,
		BearerToken: token,
		KeyID:       "kms-key-v2",
		Algorithm:   "hmac-sha256-digest",
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	if signature == "" {
		t.Fatal("empty remote signature")
	}
}

func TestRemoteCheckpointSignerRejectsMismatchedIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remoteSignResponse{
			Signature: "sig",
			KeyID:     "wrong-key",
			Algorithm: "hmac-sha256-digest",
		})
	}))
	defer server.Close()
	signer, err := NewRemoteCheckpointSigner(RemoteSignerConfig{
		Endpoint:  server.URL,
		KeyID:     "expected-key",
		Algorithm: "hmac-sha256-digest",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Sign([]byte("payload")); err == nil {
		t.Fatal("remote signer key mismatch unexpectedly accepted")
	}
}
