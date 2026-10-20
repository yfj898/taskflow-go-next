package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type issuer struct {
	mu         sync.RWMutex
	key        *rsa.PrivateKey
	kid        string
	generation int
	issuer     string
	audience   string
}

func main() {
	addr := getenv("OIDC_ADDR", ":8080")
	i := &issuer{issuer: strings.TrimRight(getenv("OIDC_ISSUER", "http://oidc-mock:8080"), "/"), audience: getenv("OIDC_AUDIENCE", "taskflow-control-plane")}
	if err := i.rotate(); err != nil {
		log.Fatal(err)
	}
	h := http.NewServeMux()
	h.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	h.HandleFunc("/jwks", i.jwks)
	h.HandleFunc("/token", i.token)
	h.HandleFunc("/rotate", func(w http.ResponseWriter, _ *http.Request) {
		if err := i.rotate(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		i.mu.RLock()
		defer i.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"kid": i.kid})
	})
	log.Printf("phase41 OIDC mock listening on %s issuer=%s", addr, i.issuer)
	log.Fatal(http.ListenAndServe(addr, h))
}

func (i *issuer) rotate() error {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.generation++
	i.key = key
	i.kid = fmt.Sprintf("phase41-k%d", i.generation)
	return nil
}

func (i *issuer) jwks(w http.ResponseWriter, _ *http.Request) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": i.kid,
		"n": base64.RawURLEncoding.EncodeToString(i.key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(i.key.E)).Bytes()),
	}}})
}

func (i *issuer) token(w http.ResponseWriter, r *http.Request) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	q := r.URL.Query()
	sub := first(q.Get("sub"), "phase41-user")
	aud := first(q.Get("aud"), i.audience)
	ttl, _ := strconv.Atoi(first(q.Get("ttl"), "300"))
	groups := []string{}
	for _, group := range strings.Split(q.Get("groups"), ",") {
		if group = strings.TrimSpace(group); group != "" {
			groups = append(groups, group)
		}
	}
	now := time.Now().UTC()
	claims := map[string]any{
		"iss": i.issuer, "aud": aud, "sub": sub,
		"iat": now.Unix(), "nbf": now.Add(-time.Second).Unix(), "exp": now.Add(time.Duration(ttl) * time.Second).Unix(),
		"groups": groups,
	}
	token, err := signJWT(i.key, i.kid, claims)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "kid": i.kid})
}

func signJWT(key *rsa.PrivateKey, kid string, claims map[string]any) (string, error) {
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	payload, _ := json.Marshal(claims)
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func first(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
