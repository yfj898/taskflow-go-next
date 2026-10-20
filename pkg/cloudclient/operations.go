package cloudclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Quota matches the TaskFlow tenant quota accounting response.
type Quota struct {
	TenantID           string `json:"tenant_id"`
	CPUMilliLimit      int64  `json:"cpu_milli_limit"`
	MemoryMiBLimit     int64  `json:"memory_mib_limit"`
	ReplicaLimit       int64  `json:"replica_limit"`
	CPUMilliUsed       int64  `json:"cpu_milli_used"`
	MemoryMiBUsed      int64  `json:"memory_mib_used"`
	ReplicaUsed        int64  `json:"replica_used"`
	CPUMilliAvailable  int64  `json:"cpu_milli_available"`
	MemoryMiBAvailable int64  `json:"memory_mib_available"`
	ReplicaAvailable   int64  `json:"replica_available"`
}

type QuotaLimits struct {
	CPUMilliLimit  int64 `json:"cpu_milli_limit"`
	MemoryMiBLimit int64 `json:"memory_mib_limit"`
	ReplicaLimit   int64 `json:"replica_limit"`
}

type ProvisionWorkflow struct {
	TenantID string          `json:"tenant_id"`
	Name     string          `json:"name"`
	Provider string          `json:"provider,omitempty"`
	Spec     json.RawMessage `json:"spec"`
}

// tenantPath rejects path delimiters and control characters rather than
// allowing a tenant supplied by a caller to alter the destination endpoint.
func tenantPath(tenant string) (string, error) {
	if tenant == "" || strings.TrimSpace(tenant) != tenant || len(tenant) > 128 {
		return "", errors.New("tenant ID must be nonempty, trimmed and at most 128 characters")
	}
	for _, r := range tenant {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return "", errors.New("tenant ID contains unsafe URL path character")
		}
	}
	return "/api/v1/cloud/tenants/" + tenant + "/quota", nil
}

func pagination(limit, offset int) (string, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return "", errors.New("pagination requires limit 1..100 and offset >= 0")
	}
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	query.Set("offset", strconv.Itoa(offset))
	return "?" + query.Encode(), nil
}

func (c *Client) GetQuota(ctx context.Context, tenant string) (*Quota, error) {
	path, err := tenantPath(tenant)
	if err != nil {
		return nil, err
	}
	var result Quota
	err = c.request(ctx, http.MethodGet, path, nil, "", &result)
	return &result, err
}

func (c *Client) SetQuota(ctx context.Context, tenant string, limits QuotaLimits, key string) (*Quota, error) {
	path, err := tenantPath(tenant)
	if err != nil {
		return nil, err
	}
	if limits.CPUMilliLimit < 0 || limits.MemoryMiBLimit < 0 || limits.ReplicaLimit < 0 {
		return nil, errors.New("quota limits must be nonnegative")
	}
	var result Quota
	err = c.request(ctx, http.MethodPut, path, limits, key, &result)
	return &result, err
}

func (c *Client) ListOrders(ctx context.Context, resourceID uint64, limit, offset int) ([]Order, error) {
	if resourceID == 0 {
		return nil, errors.New("resource ID must be positive")
	}
	query, err := pagination(limit, offset)
	if err != nil {
		return nil, err
	}
	var result struct {
		Items []Order `json:"items"`
	}
	err = c.request(ctx, http.MethodGet, resourceIDPath(resourceID)+"/orders"+query, nil, "", &result)
	return result.Items, err
}

func (c *Client) Provision(ctx context.Context, input ProvisionWorkflow, key string) (*WorkflowState, error) {
	if input.TenantID == "" || input.Name == "" || !json.Valid(input.Spec) {
		return nil, errors.New("provision requires tenant, name and valid JSON spec")
	}
	if input.Provider == "" {
		input.Provider = "kubernetes"
	}
	var result WorkflowState
	err := c.request(ctx, http.MethodPost, "/api/v1/cloud/workflows/provision", input, key, &result)
	return &result, err
}

func (c *Client) ResizeWorkflow(ctx context.Context, resourceID uint64, spec json.RawMessage, key string) (*WorkflowState, error) {
	if resourceID == 0 || !json.Valid(spec) {
		return nil, errors.New("resize requires resource ID and valid JSON spec")
	}
	var result WorkflowState
	err := c.request(ctx, http.MethodPost, resourceIDPath(resourceID)+"/workflows/resize", struct {
		Spec json.RawMessage `json:"spec"`
	}{Spec: spec}, key, &result)
	return &result, err
}

func (c *Client) DeleteWorkflow(ctx context.Context, resourceID uint64, key string) (*WorkflowState, error) {
	if resourceID == 0 {
		return nil, errors.New("resource ID must be positive")
	}
	var result WorkflowState
	err := c.request(ctx, http.MethodPost, resourceIDPath(resourceID)+"/workflows/delete", nil, key, &result)
	return &result, err
}

func (c *Client) ListWorkflows(ctx context.Context, resourceID uint64, limit, offset int) ([]Workflow, error) {
	if resourceID == 0 {
		return nil, errors.New("resource ID must be positive")
	}
	query, err := pagination(limit, offset)
	if err != nil {
		return nil, err
	}
	var result struct {
		Items []Workflow `json:"items"`
	}
	err = c.request(ctx, http.MethodGet, resourceIDPath(resourceID)+"/workflows"+query, nil, "", &result)
	return result.Items, err
}
