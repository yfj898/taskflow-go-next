package handler

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const controlPlaneRolesKey = "control_plane_roles"
const controlPlaneTenantsKey = "control_plane_tenants"

type oidcVerifier struct {
	issuer      string
	audience    string
	jwksURL     string
	groupsClaim string
	roleMap     map[string][]string
	cacheTTL    time.Duration
	clockSkew   time.Duration
	client      *http.Client

	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
	now       func() time.Time
}

type oidcJWKS struct {
	Keys []struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		Use string `json:"use"`
		Alg string `json:"alg"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func newOIDCVerifier(config ControlPlaneAuthConfig) (*oidcVerifier, error) {
	roleMap, err := parseOIDCRoleMappings(config.OIDCRoleMappings)
	if err != nil {
		return nil, err
	}
	cacheTTL := config.OIDCJWKSCacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	return &oidcVerifier{
		issuer:      strings.TrimRight(strings.TrimSpace(config.OIDCIssuer), "/"),
		audience:    strings.TrimSpace(config.OIDCAudience),
		jwksURL:     strings.TrimSpace(config.OIDCJWKSURL),
		groupsClaim: strings.TrimSpace(config.OIDCGroupsClaim),
		roleMap:     roleMap,
		cacheTTL:    cacheTTL,
		clockSkew:   config.OIDCClockSkew,
		client:      &http.Client{Timeout: 3 * time.Second},
		keys:        map[string]*rsa.PublicKey{},
		now:         time.Now,
	}, nil
}

func (v *oidcVerifier) verify(ctx context.Context, token string) (string, []string, error) {
	operator, roles, _, err := v.verifyWithTenants(ctx, token)
	return operator, roles, err
}

// The tenant grants below come exclusively from an issuer/audience/signature
// verified OIDC token, never from caller-controlled request headers.
func (v *oidcVerifier) verifyWithTenants(ctx context.Context, token string) (string, []string, []string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", nil, nil, fmt.Errorf("JWT must contain three segments")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", nil, nil, fmt.Errorf("decode JWT header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", nil, nil, fmt.Errorf("decode JWT header JSON: %w", err)
	}
	if header.Alg != "RS256" || strings.TrimSpace(header.Kid) == "" {
		return "", nil, nil, fmt.Errorf("JWT requires RS256 and kid")
	}
	key, err := v.key(ctx, header.Kid, false)
	if err != nil {
		key, err = v.key(ctx, header.Kid, true)
	}
	if err != nil {
		return "", nil, nil, err
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", nil, nil, fmt.Errorf("decode JWT signature: %w", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		return "", nil, nil, fmt.Errorf("JWT signature verification failed")
	}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", nil, nil, fmt.Errorf("decode JWT claims: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(claimsBytes)))
	decoder.UseNumber()
	claims := map[string]any{}
	if err := decoder.Decode(&claims); err != nil {
		return "", nil, nil, fmt.Errorf("decode JWT claims JSON: %w", err)
	}
	if strings.TrimRight(strings.TrimSpace(claimString(claims["iss"])), "/") != v.issuer {
		return "", nil, nil, fmt.Errorf("JWT issuer mismatch")
	}
	if !claimAudienceContains(claims["aud"], v.audience) {
		return "", nil, nil, fmt.Errorf("JWT audience mismatch")
	}
	now := v.now().UTC()
	if exp, ok := claimUnixTime(claims["exp"]); !ok || now.After(exp.Add(v.clockSkew)) {
		return "", nil, nil, fmt.Errorf("JWT is expired or missing exp")
	}
	if nbf, ok := claimUnixTime(claims["nbf"]); ok && now.Add(v.clockSkew).Before(nbf) {
		return "", nil, nil, fmt.Errorf("JWT is not yet valid")
	}
	subject := strings.TrimSpace(claimString(claims["sub"]))
	if subject == "" {
		return "", nil, nil, fmt.Errorf("JWT subject is empty")
	}
	roles := v.roles(claims[v.groupsClaim])
	unique := make(map[string]bool)
	tenants := make([]string, 0)
	for _, tenant := range append(claimStrings(claims["tenant_ids"]), claimString(claims["tenant_id"])) {
		tenant = strings.TrimSpace(tenant)
		if tenant != "" && !unique[tenant] {
			unique[tenant] = true
			tenants = append(tenants, tenant)
		}
	}
	return subject, roles, tenants, nil
}

func (v *oidcVerifier) key(ctx context.Context, kid string, force bool) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !force && v.now().Before(v.expiresAt) {
		if key := v.keys[kid]; key != nil {
			return key, nil
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch OIDC JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch OIDC JWKS: status=%d", resp.StatusCode)
	}
	var set oidcJWKS
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, fmt.Errorf("decode OIDC JWKS: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, item := range set.Keys {
		if item.Kty != "RSA" || item.Kid == "" || item.N == "" || item.E == "" || (item.Alg != "" && item.Alg != "RS256") {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(item.N)
		if err != nil {
			continue
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(item.E)
		if err != nil || len(eBytes) == 0 || len(eBytes) > 4 {
			continue
		}
		e := 0
		for _, b := range eBytes {
			e = e<<8 | int(b)
		}
		if e <= 1 {
			continue
		}
		keys[item.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("OIDC JWKS contains no usable RS256 keys")
	}
	v.keys = keys
	v.expiresAt = v.now().Add(v.cacheTTL)
	key := keys[kid]
	if key == nil {
		return nil, fmt.Errorf("OIDC JWKS does not contain kid=%q", kid)
	}
	return key, nil
}

func (v *oidcVerifier) roles(groups any) []string {
	seen := map[string]struct{}{}
	roles := make([]string, 0)
	for _, group := range claimStrings(groups) {
		for _, role := range v.roleMap[group] {
			if _, ok := seen[role]; ok {
				continue
			}
			seen[role] = struct{}{}
			roles = append(roles, role)
		}
	}
	return roles
}

func parseOIDCRoleMappings(raw string) (map[string][]string, error) {
	result := map[string][]string{}
	for _, item := range strings.Split(strings.TrimSpace(raw), ";") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid OIDC role mapping %q", item)
		}
		group := strings.TrimSpace(parts[0])
		for _, role := range strings.Split(parts[1], "|") {
			role = strings.TrimSpace(role)
			if role == "" {
				return nil, fmt.Errorf("invalid OIDC role mapping %q", item)
			}
			result[group] = append(result[group], role)
		}
	}
	return result, nil
}

func controlPlaneOIDCRBAC(config ControlPlaneAuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !config.Enabled || !config.OIDCRBACEnabled {
			c.Next()
			return
		}
		slot, _ := c.Get(controlPlaneCredentialSlotKey)
		if slot != "oidc" {
			c.Next()
			return
		}
		required := requiredControlPlaneRoles(c.Request.Method, c.FullPath())
		if len(required) == 0 {
			c.Next()
			return
		}
		actual, _ := c.Get(controlPlaneRolesKey)
		roles, _ := actual.([]string)
		for _, role := range roles {
			for _, want := range required {
				if role == want || role == "control-plane-admin" {
					c.Next()
					return
				}
			}
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden", "required_roles": required})
	}
}

func requiredControlPlaneRoles(method, path string) []string {
	if strings.Contains(path, "/receivers/break-glass") || strings.Contains(path, "/governance/dead/") && (strings.HasSuffix(path, "/requeue") || strings.HasSuffix(path, "/remediations")) || strings.HasSuffix(path, "/cancel") {
		return []string{"incident-commander"}
	}
	if strings.Contains(path, "/governance/remediations/") && (strings.HasSuffix(path, "/approve") || strings.HasSuffix(path, "/reject")) {
		return []string{"governance-reviewer"}
	}
	if strings.Contains(path, "/receivers/intents/") && strings.HasSuffix(path, "/approve") || strings.HasSuffix(path, "/execute") {
		return []string{"approver"}
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/receivers/intents") {
		return []string{"requester"}
	}
	if method == http.MethodGet {
		return []string{"control-plane-reader", "control-plane-operator"}
	}
	return []string{"control-plane-operator"}
}

func claimString(v any) string {
	s, _ := v.(string)
	return s
}

func claimStrings(v any) []string {
	switch value := v.(type) {
	case string:
		return []string{value}
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func claimAudienceContains(v any, want string) bool {
	for _, aud := range claimStrings(v) {
		if aud == want {
			return true
		}
	}
	return false
}

func claimUnixTime(v any) (time.Time, bool) {
	switch value := v.(type) {
	case json.Number:
		seconds, err := strconv.ParseInt(value.String(), 10, 64)
		if err == nil {
			return time.Unix(seconds, 0).UTC(), true
		}
	case float64:
		return time.Unix(int64(value), 0).UTC(), true
	}
	return time.Time{}, false
}
