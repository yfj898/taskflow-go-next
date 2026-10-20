package siem

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Source          string
	Expiration      time.Time
}

func (c AWSCredentials) valid(now time.Time) bool {
	if strings.TrimSpace(c.AccessKeyID) == "" || strings.TrimSpace(c.SecretAccessKey) == "" {
		return false
	}
	if c.Expiration.IsZero() {
		return true
	}
	return c.Expiration.After(now.Add(5 * time.Minute))
}

type AWSCredentialProvider interface {
	Retrieve(context.Context) (AWSCredentials, error)
}

type StaticAWSCredentialProvider struct {
	Credentials AWSCredentials
}

func (p StaticAWSCredentialProvider) Retrieve(context.Context) (AWSCredentials, error) {
	creds := p.Credentials
	creds.AccessKeyID = strings.TrimSpace(creds.AccessKeyID)
	creds.SecretAccessKey = strings.TrimSpace(creds.SecretAccessKey)
	creds.SessionToken = strings.TrimSpace(creds.SessionToken)
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return AWSCredentials{}, errors.New("static AWS credentials are incomplete")
	}
	if creds.Source == "" {
		creds.Source = "static"
	}
	return creds, nil
}

type ChainedAWSCredentialProvider struct {
	providers []AWSCredentialProvider
}

func NewChainedAWSCredentialProvider(providers ...AWSCredentialProvider) (*ChainedAWSCredentialProvider, error) {
	filtered := make([]AWSCredentialProvider, 0, len(providers))
	for _, provider := range providers {
		if provider != nil {
			filtered = append(filtered, provider)
		}
	}
	if len(filtered) == 0 {
		return nil, errors.New("AWS credential chain has no providers")
	}
	return &ChainedAWSCredentialProvider{providers: filtered}, nil
}

func (p *ChainedAWSCredentialProvider) Retrieve(ctx context.Context) (AWSCredentials, error) {
	var failures []error
	for _, provider := range p.providers {
		creds, err := provider.Retrieve(ctx)
		if err == nil {
			return creds, nil
		}
		failures = append(failures, err)
	}
	return AWSCredentials{}, fmt.Errorf("AWS credential chain exhausted: %w", errors.Join(failures...))
}

type AWSWebIdentityProviderConfig struct {
	TokenFile       string
	RoleARN         string
	RoleSessionName string
	STSEndpoint     string
	Timeout         time.Duration
}

type AWSWebIdentityProvider struct {
	config AWSWebIdentityProviderConfig
	client *http.Client
	mu     sync.Mutex
	cache  AWSCredentials
}

type assumeRoleWithWebIdentityResponse struct {
	Result struct {
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
	} `xml:"AssumeRoleWithWebIdentityResult"`
}

func NewAWSWebIdentityProvider(config AWSWebIdentityProviderConfig) (*AWSWebIdentityProvider, error) {
	config.TokenFile = strings.TrimSpace(config.TokenFile)
	config.RoleARN = strings.TrimSpace(config.RoleARN)
	config.RoleSessionName = strings.TrimSpace(config.RoleSessionName)
	config.STSEndpoint = strings.TrimSpace(config.STSEndpoint)
	if config.TokenFile == "" || config.RoleARN == "" {
		return nil, errors.New("AWS web identity token file and role ARN are required")
	}
	if !filepath.IsAbs(config.TokenFile) {
		return nil, errors.New("AWS web identity token file must be an absolute path")
	}
	if config.RoleSessionName == "" {
		config.RoleSessionName = "taskflow-siem"
	}
	if config.STSEndpoint == "" {
		config.STSEndpoint = "https://sts.amazonaws.com"
	}
	u, err := url.Parse(config.STSEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("AWS STS endpoint must be an absolute http(s) URL")
	}
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}
	return &AWSWebIdentityProvider{config: config, client: &http.Client{Timeout: config.Timeout}}, nil
}

func (p *AWSWebIdentityProvider) Retrieve(ctx context.Context) (AWSCredentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache.valid(time.Now().UTC()) {
		return p.cache, nil
	}
	tokenBytes, err := os.ReadFile(p.config.TokenFile)
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("read AWS web identity token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return AWSCredentials{}, errors.New("AWS web identity token is empty")
	}
	form := url.Values{}
	form.Set("Action", "AssumeRoleWithWebIdentity")
	form.Set("Version", "2011-06-15")
	form.Set("RoleArn", p.config.RoleARN)
	form.Set("RoleSessionName", p.config.RoleSessionName)
	form.Set("WebIdentityToken", token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.STSEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("build AWS STS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("AWS STS AssumeRoleWithWebIdentity request: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 128*1024))
	if readErr != nil {
		return AWSCredentials{}, fmt.Errorf("read AWS STS response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return AWSCredentials{}, fmt.Errorf("AWS STS AssumeRoleWithWebIdentity returned HTTP %d", resp.StatusCode)
	}
	var result assumeRoleWithWebIdentityResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return AWSCredentials{}, fmt.Errorf("decode AWS STS response: %w", err)
	}
	expiration, err := time.Parse(time.RFC3339, strings.TrimSpace(result.Result.Credentials.Expiration))
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("decode AWS STS credential expiration: %w", err)
	}
	creds := AWSCredentials{
		AccessKeyID:     strings.TrimSpace(result.Result.Credentials.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(result.Result.Credentials.SecretAccessKey),
		SessionToken:    strings.TrimSpace(result.Result.Credentials.SessionToken),
		Source:          "web-identity",
		Expiration:      expiration.UTC(),
	}
	if !creds.valid(time.Now().UTC()) {
		return AWSCredentials{}, errors.New("AWS STS returned incomplete or nearly-expired credentials")
	}
	p.cache = creds
	return creds, nil
}

type AWSIMDSv2ProviderConfig struct {
	Endpoint string
	Timeout  time.Duration
}

type AWSIMDSv2Provider struct {
	config AWSIMDSv2ProviderConfig
	client *http.Client
	mu     sync.Mutex
	cache  AWSCredentials
}

type imdsCredentialResponse struct {
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
}

func NewAWSIMDSv2Provider(config AWSIMDSv2ProviderConfig) (*AWSIMDSv2Provider, error) {
	config.Endpoint = strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	if config.Endpoint == "" {
		config.Endpoint = "http://169.254.169.254"
	}
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("AWS IMDS endpoint must be an absolute http(s) URL")
	}
	if config.Timeout <= 0 {
		config.Timeout = time.Second
	}
	return &AWSIMDSv2Provider{config: config, client: &http.Client{Timeout: config.Timeout}}, nil
}

func (p *AWSIMDSv2Provider) Retrieve(ctx context.Context) (AWSCredentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache.valid(time.Now().UTC()) {
		return p.cache, nil
	}
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodPut, p.config.Endpoint+"/latest/api/token", nil)
	if err != nil {
		return AWSCredentials{}, err
	}
	tokenReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tokenResp, err := p.client.Do(tokenReq)
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("AWS IMDSv2 token request: %w", err)
	}
	tokenBody, readErr := io.ReadAll(io.LimitReader(tokenResp.Body, 16*1024))
	tokenResp.Body.Close()
	if readErr != nil {
		return AWSCredentials{}, fmt.Errorf("read AWS IMDSv2 token: %w", readErr)
	}
	if tokenResp.StatusCode < 200 || tokenResp.StatusCode >= 300 {
		return AWSCredentials{}, fmt.Errorf("AWS IMDSv2 token returned HTTP %d", tokenResp.StatusCode)
	}
	token := strings.TrimSpace(string(tokenBody))
	if token == "" {
		return AWSCredentials{}, errors.New("AWS IMDSv2 returned empty token")
	}
	roleName, err := p.getMetadata(ctx, token, "/latest/meta-data/iam/security-credentials/")
	if err != nil {
		return AWSCredentials{}, err
	}
	roleName = strings.TrimSpace(strings.Split(roleName, "\n")[0])
	if roleName == "" || strings.Contains(roleName, "/") {
		return AWSCredentials{}, errors.New("AWS IMDSv2 returned invalid role name")
	}
	credentialJSON, err := p.getMetadata(ctx, token, "/latest/meta-data/iam/security-credentials/"+url.PathEscape(roleName))
	if err != nil {
		return AWSCredentials{}, err
	}
	var result imdsCredentialResponse
	if err := json.Unmarshal([]byte(credentialJSON), &result); err != nil {
		return AWSCredentials{}, fmt.Errorf("decode AWS IMDSv2 credentials: %w", err)
	}
	expiration, err := time.Parse(time.RFC3339, strings.TrimSpace(result.Expiration))
	if err != nil {
		return AWSCredentials{}, fmt.Errorf("decode AWS IMDSv2 credential expiration: %w", err)
	}
	creds := AWSCredentials{
		AccessKeyID:     strings.TrimSpace(result.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(result.SecretAccessKey),
		SessionToken:    strings.TrimSpace(result.Token),
		Source:          "imds-v2",
		Expiration:      expiration.UTC(),
	}
	if !creds.valid(time.Now().UTC()) {
		return AWSCredentials{}, errors.New("AWS IMDSv2 returned incomplete or nearly-expired credentials")
	}
	p.cache = creds
	return creds, nil
}

func (p *AWSIMDSv2Provider) getMetadata(ctx context.Context, token, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.config.Endpoint+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)
	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("AWS IMDSv2 metadata request: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return "", fmt.Errorf("read AWS IMDSv2 metadata response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("AWS IMDSv2 metadata returned HTTP %d", resp.StatusCode)
	}
	return string(body), nil
}
