package siem

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVaultTransitSignerSendsPrehashedDeterministicRSARequest(t *testing.T) {
	payload := []byte("phase28-audit-envelope")
	wantDigest := sha256.Sum256(payload)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/transit/sign/taskflow-audit/sha2-256" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Header.Get("X-Vault-Token") != "vault-token" {
			t.Fatal("missing Vault token")
		}
		var request vaultTransitSignRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(request.Input)
		if err != nil {
			t.Fatal(err)
		}
		if string(decoded) != string(wantDigest[:]) || !request.Prehashed || request.SignatureAlgorithm != "pkcs1v15" {
			t.Fatalf("request = %+v", request)
		}
		_, _ = w.Write([]byte(`{"data":{"signature":"vault:v3:c2lnbmF0dXJl","key_version":3}}`))
	}))
	defer server.Close()

	signer, err := NewVaultTransitSigner(VaultTransitSignerConfig{
		Endpoint:   server.URL,
		Token:      "vault-token",
		Mount:      "transit",
		KeyName:    "taskflow-audit",
		KeyID:      "phase28-vault-v3",
		KeyVersion: 3,
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	if signature != "vault:v3:c2lnbmF0dXJl" {
		t.Fatalf("signature = %q", signature)
	}
	if signer.Algorithm() != "vault-transit-rsa-pkcs1v15-sha2-256-prehashed" {
		t.Fatalf("algorithm = %q", signer.Algorithm())
	}
}
