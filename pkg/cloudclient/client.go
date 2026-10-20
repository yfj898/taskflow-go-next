// Package cloudclient provides an embeddable Go HTTP client for TaskFlow's
// cloud resource control APIs. It sends explicit idempotency keys on writes,
// respects context cancellation, and never blindly retries a mutation.
package cloudclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 4 << 20

// TokenSource can fetch and rotate short-lived credentials for a caller.
type TokenSource func(context.Context) (string, error)

type Client struct {
	endpoint *url.URL
	http     *http.Client
	token    TokenSource
}

type APIError struct {
	StatusCode int
	RequestID  string
	Message    string
}

func (e *APIError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("taskflow HTTP %d (request_id=%s): %s", e.StatusCode, e.RequestID, e.Message)
	}
	return fmt.Sprintf("taskflow HTTP %d: %s", e.StatusCode, e.Message)
}

// New requires a concrete endpoint and a credential source. Callers may pass
// a custom http.Client for TLS, proxy, metrics, transport testing or timeout.
func New(endpoint string, token TokenSource, httpClient *http.Client) (*Client, error) {
	base, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || base == nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("invalid TaskFlow HTTP endpoint")
	}
	if token == nil {
		return nil, errors.New("TaskFlow token source is required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{endpoint: base, http: httpClient, token: token}, nil
}

type Resource struct {
	ID                uint64 `json:"id"`
	TenantID          string `json:"tenant_id"`
	Name              string `json:"name"`
	Generation        uint64 `json:"generation"`
	DesiredState      string `json:"desired_state"`
	ActualState       string `json:"actual_state"`
	OrchestrationMode string `json:"orchestration_mode"`
}

type Order struct {
	ID             uint64 `json:"id"`
	ResourceID     uint64 `json:"resource_id"`
	TenantID       string `json:"tenant_id"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotency_key"`
}

type Mutation struct {
	Resource Resource `json:"resource"`
	Order    Order    `json:"order"`
}

type Workflow struct {
	ID       uint64 `json:"id"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
	Kind     string `json:"kind"`
}

type WorkflowState struct {
	Workflow Workflow          `json:"workflow"`
	Resource Resource          `json:"resource"`
	Order    Order             `json:"order"`
	Steps    []json.RawMessage `json:"steps"`
}

type CreateResource struct {
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
	Kind     string `json:"kind,omitempty"`
	Provider string `json:"provider,omitempty"`
	Spec     any    `json:"spec"`
}

// request issues one request, with no automatic retries of resource mutations.
// A nonempty idempotency key is required for all mutation methods.
func (c *Client) request(ctx context.Context, method, path string, payload any, idempotencyKey string, output any) error {
	if method != http.MethodGet && strings.TrimSpace(idempotencyKey) == "" {
		return errors.New("mutation requires a stable idempotency key")
	}
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *c.endpoint
	pathPart, rawQuery, hasQuery := strings.Cut(path, "?")
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + pathPart
	endpoint.RawPath = ""
	if hasQuery {
		endpoint.RawQuery = rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return err
	}
	token, err := c.token(ctx)
	if err != nil {
		return fmt.Errorf("fetch TaskFlow token: %w", err)
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("empty TaskFlow token")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read TaskFlow response: %w", err)
	}
	if len(data) > maxResponseBytes {
		return errors.New("TaskFlow response exceeded 4 MiB limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiError struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &apiError)
		if apiError.Error == "" {
			apiError.Error = http.StatusText(resp.StatusCode)
		}
		return &APIError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get("X-Request-ID"), Message: apiError.Error}
	}
	if output != nil {
		if err := json.Unmarshal(data, output); err != nil {
			return fmt.Errorf("decode TaskFlow response: %w", err)
		}
	}
	return nil
}

func resourceIDPath(id uint64) string { return "/api/v1/cloud/resources/" + strconv.FormatUint(id, 10) }

func (c *Client) CreateResource(ctx context.Context, in CreateResource, key string) (*Mutation, error) {
	var result Mutation
	err := c.request(ctx, http.MethodPost, "/api/v1/cloud/resources", in, key, &result)
	return &result, err
}

func (c *Client) GetResource(ctx context.Context, id uint64) (*Resource, error) {
	var resource Resource
	err := c.request(ctx, http.MethodGet, resourceIDPath(id), nil, "", &resource)
	return &resource, err
}

func (c *Client) ListResources(ctx context.Context, tenant string, limit, offset int) ([]Resource, error) {
	if tenant == "" {
		return nil, errors.New("tenant must be explicit when listing resources")
	}
	query := url.Values{}
	query.Set("tenant_id", tenant)
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	var result struct {
		Items []Resource `json:"items"`
	}
	err := c.request(ctx, http.MethodGet, "/api/v1/cloud/resources?"+query.Encode(), nil, "", &result)
	return result.Items, err
}

func (c *Client) DeleteResource(ctx context.Context, id uint64, key string) (*Mutation, error) {
	var result Mutation
	err := c.request(ctx, http.MethodDelete, resourceIDPath(id), nil, key, &result)
	return &result, err
}

func (c *Client) GetWorkflow(ctx context.Context, id uint64) (*WorkflowState, error) {
	var state WorkflowState
	err := c.request(ctx, http.MethodGet, "/api/v1/cloud/workflows/"+strconv.FormatUint(id, 10), nil, "", &state)
	return &state, err
}

// WaitWorkflow waits until a durable workflow succeeds or reaches a terminal
// error/compensation state. It never starts or forces reconciliation; the
// controller is responsible for progress.
func (c *Client) WaitWorkflow(ctx context.Context, id uint64, interval time.Duration) (*WorkflowState, error) {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		state, err := c.GetWorkflow(ctx, id)
		if err != nil {
			return nil, err
		}
		switch state.Workflow.Status {
		case "succeeded":
			return state, nil
		case "compensated", "failed":
			return state, fmt.Errorf("workflow %d terminal status %s", id, state.Workflow.Status)
		}
		timer.Reset(interval)
	}
}
