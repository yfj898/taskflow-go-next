package siem

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestHMACSHA256Signer(t *testing.T) {
	signer, err := NewHMACSHA256Signer("audit-key-v1", "phase25-secret")
	if err != nil {
		t.Fatalf("NewHMACSHA256Signer() error = %v", err)
	}
	payload := []byte(`{"checkpoint":42}`)
	got, err := signer.Sign(payload)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	mac := hmac.New(sha256.New, []byte("phase25-secret"))
	_, _ = mac.Write(payload)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(got), []byte(want)) {
		t.Fatalf("signature = %q, want %q", got, want)
	}
	if signer.KeyID() != "audit-key-v1" {
		t.Fatalf("KeyID() = %q", signer.KeyID())
	}
}

func TestHMACSHA256SignerRejectsEmptyConfiguration(t *testing.T) {
	if _, err := NewHMACSHA256Signer("", "secret"); err == nil {
		t.Fatal("empty key id unexpectedly accepted")
	}
	if _, err := NewHMACSHA256Signer("id", strings.Repeat(" ", 3)); err == nil {
		t.Fatal("empty key unexpectedly accepted")
	}
}

func TestRotatingHMACSHA256SignerProducesActiveAndPreviousSignatures(t *testing.T) {
	signer, err := NewRotatingHMACSHA256Signer("key-v2", "active-secret", "key-v1", "previous-secret")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("phase26-checkpoint")
	active, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := signer.SignPrevious(payload)
	if err != nil {
		t.Fatal(err)
	}
	if active == previous {
		t.Fatal("active and previous signatures unexpectedly match")
	}
	if signer.KeyID() != "key-v2" || signer.PreviousKeyID() != "key-v1" {
		t.Fatalf("unexpected key ids: active=%q previous=%q", signer.KeyID(), signer.PreviousKeyID())
	}
	if signer.Algorithm() != "hmac-sha256" || signer.PreviousAlgorithm() != "hmac-sha256" {
		t.Fatalf("unexpected algorithms: active=%q previous=%q", signer.Algorithm(), signer.PreviousAlgorithm())
	}
}

func TestRotatingHMACSHA256SignerRejectsUnsafeOverlap(t *testing.T) {
	if _, err := NewRotatingHMACSHA256Signer("key-v2", "active", "key-v2", "old"); err == nil {
		t.Fatal("duplicate key id unexpectedly accepted")
	}
	if _, err := NewRotatingHMACSHA256Signer("key-v2", "active", "key-v1", "active"); err == nil {
		t.Fatal("duplicate key material unexpectedly accepted")
	}
	if _, err := NewRotatingHMACSHA256Signer("key-v2", "active", "key-v1", ""); err == nil {
		t.Fatal("partial previous-key configuration unexpectedly accepted")
	}
}
