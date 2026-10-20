package handler

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestControlPlaneOIDCJWTAndRBAC(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "phase41-key"
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	config := ControlPlaneAuthConfig{
		Enabled: true, Mode: "oidc",
		OIDCIssuer: "https://issuer.example", OIDCAudience: "taskflow-control-plane", OIDCJWKSURL: jwks.URL,
		OIDCGroupsClaim: "groups", OIDCRoleMappings: "ops=control-plane-reader|control-plane-operator;review=governance-reviewer",
		OIDCJWKSCacheTTL: time.Minute, OIDCRBACEnabled: true,
	}
	r.Use(controlPlaneBearerAuth(config), controlPlaneOIDCRBAC(config))
	r.GET("/api/v1/control-plane/status", func(c *gin.Context) {
		operator, _ := c.Get(controlPlaneOperatorKey)
		c.JSON(http.StatusOK, gin.H{"operator": operator})
	})
	r.POST("/api/v1/control-plane/mutation", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	now := time.Now().UTC()
	token := signPhase41JWT(t, key, kid, map[string]any{
		"iss": "https://issuer.example", "aud": "taskflow-control-plane", "sub": "alice",
		"exp": now.Add(time.Minute).Unix(), "groups": []string{"ops"},
	})

	for _, tc := range []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodGet, "/api/v1/control-plane/status", http.StatusOK},
		{http.MethodPost, "/api/v1/control-plane/mutation", http.StatusNoContent},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		if res.Code != tc.want {
			t.Fatalf("%s %s status=%d body=%s", tc.method, tc.path, res.Code, res.Body.String())
		}
	}
}

func TestControlPlaneOIDCRejectsExpiredAndWrongAudience(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": "k1", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()
	v, err := newOIDCVerifier(ControlPlaneAuthConfig{
		OIDCIssuer: "https://issuer", OIDCAudience: "good", OIDCJWKSURL: jwks.URL, OIDCGroupsClaim: "groups",
		OIDCJWKSCacheTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, claims := range []map[string]any{
		{"iss": "https://issuer", "aud": "good", "sub": "alice", "exp": now.Add(-time.Minute).Unix()},
		{"iss": "https://issuer", "aud": "wrong", "sub": "alice", "exp": now.Add(time.Minute).Unix()},
	} {
		token := signPhase41JWT(t, key, "k1", claims)
		if _, _, err := v.verify(t.Context(), token); err == nil {
			t.Fatalf("expected token rejection for claims=%v", claims)
		}
	}
}

func signPhase41JWT(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	payload, _ := json.Marshal(claims)
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join([]string{input, base64.RawURLEncoding.EncodeToString(sig)}, ".")
}
