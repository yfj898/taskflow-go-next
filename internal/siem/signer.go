package siem

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

type CheckpointSigner interface {
	KeyID() string
	Sign([]byte) (string, error)
}

type SignatureAlgorithmProvider interface {
	Algorithm() string
}

type PreviousSignatureProvider interface {
	PreviousKeyID() string
	PreviousAlgorithm() string
	SignPrevious([]byte) (string, error)
}

type HMACSHA256Signer struct {
	keyID string
	key   []byte
}

func NewHMACSHA256Signer(keyID, key string) (*HMACSHA256Signer, error) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, errors.New("SIEM signing key id is empty")
	}
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("SIEM signing key is empty")
	}
	return &HMACSHA256Signer{keyID: keyID, key: []byte(key)}, nil
}

func (s *HMACSHA256Signer) KeyID() string { return s.keyID }

func (s *HMACSHA256Signer) Algorithm() string { return "hmac-sha256" }

func (s *HMACSHA256Signer) Sign(payload []byte) (string, error) {
	mac := hmac.New(sha256.New, s.key)
	if _, err := mac.Write(payload); err != nil {
		return "", err
	}
	return "sha256=" + hex.EncodeToString(mac.Sum(nil)), nil
}

type RotatingHMACSHA256Signer struct {
	active   *HMACSHA256Signer
	previous *HMACSHA256Signer
}

func NewRotatingHMACSHA256Signer(activeKeyID, activeKey, previousKeyID, previousKey string) (*RotatingHMACSHA256Signer, error) {
	active, err := NewHMACSHA256Signer(activeKeyID, activeKey)
	if err != nil {
		return nil, err
	}
	previousKeyID = strings.TrimSpace(previousKeyID)
	previousKey = strings.TrimSpace(previousKey)
	if (previousKeyID == "") != (previousKey == "") {
		return nil, errors.New("previous SIEM signing key id and key must be configured together")
	}
	var previous *HMACSHA256Signer
	if previousKeyID != "" {
		if previousKeyID == active.KeyID() {
			return nil, errors.New("previous SIEM signing key id must differ from active key id")
		}
		if previousKey == strings.TrimSpace(activeKey) {
			return nil, errors.New("previous SIEM signing key must differ from active key")
		}
		previous, err = NewHMACSHA256Signer(previousKeyID, previousKey)
		if err != nil {
			return nil, err
		}
	}
	return &RotatingHMACSHA256Signer{active: active, previous: previous}, nil
}

func (s *RotatingHMACSHA256Signer) KeyID() string { return s.active.KeyID() }

func (s *RotatingHMACSHA256Signer) Algorithm() string { return s.active.Algorithm() }

func (s *RotatingHMACSHA256Signer) Sign(payload []byte) (string, error) {
	return s.active.Sign(payload)
}

func (s *RotatingHMACSHA256Signer) PreviousKeyID() string {
	if s.previous == nil {
		return ""
	}
	return s.previous.KeyID()
}

func (s *RotatingHMACSHA256Signer) PreviousAlgorithm() string {
	if s.previous == nil {
		return ""
	}
	return s.previous.Algorithm()
}

func (s *RotatingHMACSHA256Signer) SignPrevious(payload []byte) (string, error) {
	if s.previous == nil {
		return "", nil
	}
	return s.previous.Sign(payload)
}
