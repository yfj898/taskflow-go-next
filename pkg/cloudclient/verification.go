package cloudclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type VerificationRequest struct {
	RunID                string   `json:"run_id"`
	ProjectID            string   `json:"project_id"`
	TargetCommit         string   `json:"target_commit"`
	CheckProfileID       string   `json:"check_profile_id"`
	CheckProfileRevision string   `json:"check_profile_revision"`
	CheckIDs             []string `json:"check_ids"`
	ExternalSnapshotID   string   `json:"external_snapshot_id"`
}
type VerificationCheck struct {
	ID         string `json:"id"`
	Required   bool   `json:"required"`
	Status     string `json:"status"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	Error      string `json:"error,omitempty"`
}
type VerificationReport struct {
	Generation uint64              `json:"generation"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt *time.Time          `json:"finished_at,omitempty"`
	Verdict    string              `json:"verdict"`
	Error      string              `json:"error,omitempty"`
	Checks     []VerificationCheck `json:"checks"`
}
type VerificationRun struct {
	VerificationRequest
	TaskID     uint64               `json:"task_id"`
	Status     string               `json:"status"`
	Generation uint64               `json:"generation"`
	LastError  string               `json:"last_error,omitempty"`
	Attempts   []VerificationReport `json:"attempts"`
	CreatedAt  time.Time            `json:"created_at"`
}

func (c *Client) SubmitVerification(ctx context.Context, input VerificationRequest) (*VerificationRun, error) {
	var out VerificationRun
	if strings.TrimSpace(input.RunID) == "" {
		return nil, fmt.Errorf("verification submit requires stable run UUID")
	}
	err := c.request(ctx, http.MethodPost, "/api/v1/verification-runs", input, input.RunID, &out)
	return &out, err
}
func (c *Client) GetVerificationRun(ctx context.Context, runID string) (*VerificationRun, error) {
	var out VerificationRun
	err := c.request(ctx, http.MethodGet, "/api/v1/verification-runs/"+url.PathEscape(runID), nil, "", &out)
	return &out, err
}
func (c *Client) GetVerificationArtifact(ctx context.Context, runID string, generation uint64, artifactID string) ([]byte, error) {
	endpoint := *c.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/v1/verification-runs/" + runID + "/artifacts/" + artifactID
	endpoint.RawPath = strings.TrimRight(c.endpoint.EscapedPath(), "/") + "/api/v1/verification-runs/" + url.PathEscape(runID) + "/artifacts/" + url.PathEscape(artifactID)
	endpoint.RawQuery = "generation=" + strconv.FormatUint(generation, 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	token, err := c.token(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch TaskFlow token: %w", err)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("empty TaskFlow token")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/plain")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
		if err != nil {
			return nil, fmt.Errorf("read TaskFlow error response: %w", err)
		}
		if len(data) > maxResponseBytes {
			return nil, fmt.Errorf("TaskFlow error response exceeded 4 MiB limit")
		}
		var body struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &body)
		if body.Error == "" {
			body.Error = http.StatusText(resp.StatusCode)
		}
		return nil, &APIError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get("X-Request-ID"), Message: body.Error}
	}
	return io.ReadAll(resp.Body)
}
