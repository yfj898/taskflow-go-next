package siem

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAWSKMSMACSignerUsesDigestAndSigV4(t *testing.T) {
	const kmsKey = "kms-managed-test-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") != "TrentService.GenerateMac" {
			t.Fatalf("X-Amz-Target = %q", r.Header.Get("X-Amz-Target"))
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/") {
			t.Fatalf("missing SigV4 Authorization: %q", r.Header.Get("Authorization"))
		}
		var request awsKMSGenerateMACRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		message, err := base64.StdEncoding.DecodeString(request.Message)
		if err != nil || len(message) != sha256.Size {
			t.Fatalf("GenerateMac message is not a SHA-256 digest: %q", request.Message)
		}
		mac := hmac.New(sha256.New, []byte(kmsKey))
		_, _ = mac.Write(message)
		_ = json.NewEncoder(w).Encode(awsKMSGenerateMACResponse{
			KeyID:        "kms-key-1",
			MACAlgorithm: "HMAC_SHA_256",
			MAC:          base64.StdEncoding.EncodeToString(mac.Sum(nil)),
		})
	}))
	defer server.Close()

	signer, err := NewAWSKMSMACSigner(AWSKMSMACSignerConfig{
		Endpoint:        server.URL,
		Region:          "ap-southeast-1",
		AccessKeyID:     "AKID",
		SecretAccessKey: "SECRET",
		KeyID:           "kms-key-1",
		Timeout:         time.Second,
		Now:             func() time.Time { return time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.Sign([]byte("audit-envelope"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(signature, "base64=") {
		t.Fatalf("signature = %q", signature)
	}
	if signer.Algorithm() != "aws-kms-HMAC_SHA_256-digest" {
		t.Fatalf("Algorithm() = %q", signer.Algorithm())
	}
}
