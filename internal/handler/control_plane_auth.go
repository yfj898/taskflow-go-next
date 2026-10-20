package handler

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type ControlPlaneAuthConfig struct {
	Enabled                     bool
	BearerToken                 string
	PreviousBearerToken         string
	AdditionalBearerCredentials string
	OperatorID                  string
	PreviousOperatorID          string
	Mode                        string
	OIDCIssuer                  string
	OIDCAudience                string
	OIDCJWKSURL                 string
	OIDCGroupsClaim             string
	OIDCRoleMappings            string
	OIDCJWKSCacheTTL            time.Duration
	OIDCClockSkew               time.Duration
	OIDCRBACEnabled             bool
	CloudTenantAuthzEnabled     bool
}

type controlPlaneAdditionalCredential struct {
	operator string
	hash     [32]byte
}

const (
	controlPlaneOperatorKey       = "control_plane_operator_id"
	controlPlaneCredentialSlotKey = "control_plane_credential_slot"
)

func controlPlaneBearerAuth(config ControlPlaneAuthConfig) gin.HandlerFunc {
	mode := strings.ToLower(strings.TrimSpace(config.Mode))
	if mode == "" {
		mode = "bearer"
	}
	bearerEnabled := mode == "bearer" || mode == "hybrid"
	oidcEnabled := mode == "oidc" || mode == "hybrid"
	activeToken := strings.TrimSpace(config.BearerToken)
	previousToken := strings.TrimSpace(config.PreviousBearerToken)
	activeExpected := sha256.Sum256([]byte(activeToken))
	previousExpected := sha256.Sum256([]byte(previousToken))
	activeOperator := strings.TrimSpace(config.OperatorID)
	if activeOperator == "" {
		activeOperator = "operator"
	}
	previousOperator := strings.TrimSpace(config.PreviousOperatorID)
	if previousOperator == "" {
		previousOperator = activeOperator
	}
	additional, additionalOK := parseControlPlaneAdditionalCredentials(config.AdditionalBearerCredentials)
	var verifier *oidcVerifier
	var oidcErr error
	if config.Enabled && oidcEnabled {
		verifier, oidcErr = newOIDCVerifier(config)
	}
	misconfigured := config.Enabled && (!additionalOK || (bearerEnabled && activeToken == "") || (oidcEnabled && oidcErr != nil))

	return func(c *gin.Context) {
		if !config.Enabled {
			c.Set(controlPlaneOperatorKey, "unauthenticated")
			c.Set(controlPlaneCredentialSlotKey, "disabled")
			c.Next()
			return
		}
		if misconfigured {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "control-plane authentication is misconfigured"})
			return
		}

		provided, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok {
			c.Header("WWW-Authenticate", "Bearer")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		actual := sha256.Sum256([]byte(provided))
		activeMatch := 0
		if bearerEnabled {
			activeMatch = subtle.ConstantTimeCompare(activeExpected[:], actual[:])
		}
		previousMatch := 0
		if bearerEnabled && previousToken != "" {
			previousMatch = subtle.ConstantTimeCompare(previousExpected[:], actual[:])
		}
		additionalOperator := ""
		if bearerEnabled {
			for _, credential := range additional {
				if subtle.ConstantTimeCompare(credential.hash[:], actual[:]) == 1 {
					additionalOperator = credential.operator
				}
			}
		}
		if activeMatch != 1 && previousMatch != 1 && additionalOperator == "" && oidcEnabled {
			operator, roles, tenants, err := verifier.verifyWithTenants(c.Request.Context(), provided)
			if err == nil {
				c.Set(controlPlaneOperatorKey, operator)
				c.Set(controlPlaneCredentialSlotKey, "oidc")
				c.Set(controlPlaneRolesKey, roles)
				c.Set(controlPlaneTenantsKey, tenants)
				c.Next()
				return
			}
		}
		if activeMatch != 1 && previousMatch != 1 && additionalOperator == "" {
			c.Header("WWW-Authenticate", "Bearer")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		if activeMatch == 1 {
			c.Set(controlPlaneOperatorKey, activeOperator)
			c.Set(controlPlaneCredentialSlotKey, "active")
		} else if previousMatch == 1 {
			c.Set(controlPlaneOperatorKey, previousOperator)
			c.Set(controlPlaneCredentialSlotKey, "previous")
		} else {
			c.Set(controlPlaneOperatorKey, additionalOperator)
			c.Set(controlPlaneCredentialSlotKey, "additional")
		}
		c.Next()
	}
}

func parseControlPlaneAdditionalCredentials(raw string) ([]controlPlaneAdditionalCredential, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	seenOperators := map[string]struct{}{}
	credentials := make([]controlPlaneAdditionalCredential, 0)
	for _, item := range strings.Split(raw, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return nil, false
		}
		operator := strings.TrimSpace(parts[0])
		token := strings.TrimSpace(parts[1])
		if operator == "" || token == "" {
			return nil, false
		}
		if _, exists := seenOperators[operator]; exists {
			return nil, false
		}
		seenOperators[operator] = struct{}{}
		credentials = append(credentials, controlPlaneAdditionalCredential{
			operator: operator,
			hash:     sha256.Sum256([]byte(token)),
		})
	}
	return credentials, true
}

func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}
