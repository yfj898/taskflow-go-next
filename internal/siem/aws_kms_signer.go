package siem

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type AWSKMSMACSignerConfig struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Credentials     AWSCredentialProvider
	KeyID           string
	Timeout         time.Duration
	Now             func() time.Time
}

type AWSKMSMACSigner struct {
	config      AWSKMSMACSignerConfig
	credentials AWSCredentialProvider
	client      *http.Client
}

type awsKMSGenerateMACRequest struct {
	KeyID        string `json:"KeyId"`
	MACAlgorithm string `json:"MacAlgorithm"`
	Message      string `json:"Message"`
}

type awsKMSGenerateMACResponse struct {
	KeyID        string `json:"KeyId"`
	MACAlgorithm string `json:"MacAlgorithm"`
	MAC          string `json:"Mac"`
}

func NewAWSKMSMACSigner(config AWSKMSMACSignerConfig) (*AWSKMSMACSigner, error) {
	config.Region = strings.TrimSpace(config.Region)
	config.AccessKeyID = strings.TrimSpace(config.AccessKeyID)
	config.SecretAccessKey = strings.TrimSpace(config.SecretAccessKey)
	config.KeyID = strings.TrimSpace(config.KeyID)
	if config.Region == "" {
		return nil, fmt.Errorf("AWS KMS region is empty")
	}
	if config.KeyID == "" {
		return nil, fmt.Errorf("AWS KMS key id is empty")
	}
	credentials := config.Credentials
	if credentials == nil {
		if config.AccessKeyID == "" || config.SecretAccessKey == "" {
			return nil, fmt.Errorf("AWS KMS credentials are incomplete")
		}
		credentials = StaticAWSCredentialProvider{Credentials: AWSCredentials{
			AccessKeyID:     config.AccessKeyID,
			SecretAccessKey: config.SecretAccessKey,
			SessionToken:    config.SessionToken,
			Source:          "static",
		}}
	}
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if config.Endpoint == "" {
		config.Endpoint = "https://kms." + config.Region + ".amazonaws.com"
	}
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("AWS KMS endpoint must be an absolute http(s) URL")
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("AWS KMS endpoint must not contain a path")
	}
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &AWSKMSMACSigner{config: config, credentials: credentials, client: &http.Client{Timeout: config.Timeout}}, nil
}

func (s *AWSKMSMACSigner) KeyID() string { return s.config.KeyID }

func (s *AWSKMSMACSigner) Algorithm() string { return "aws-kms-HMAC_SHA_256-digest" }

func (s *AWSKMSMACSigner) Sign(payload []byte) (string, error) {
	credentials, err := s.credentials.Retrieve(context.Background())
	if err != nil {
		return "", fmt.Errorf("retrieve AWS KMS credentials: %w", err)
	}
	digest := sha256.Sum256(payload)
	body, err := json.Marshal(awsKMSGenerateMACRequest{
		KeyID:        s.config.KeyID,
		MACAlgorithm: "HMAC_SHA_256",
		Message:      base64.StdEncoding.EncodeToString(digest[:]),
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, s.config.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build AWS KMS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService.GenerateMac")
	if err := s.signV4(req, body, s.config.Now().UTC(), credentials); err != nil {
		return "", err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("AWS KMS GenerateMac request: %w", err)
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return "", fmt.Errorf("read AWS KMS response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("AWS KMS GenerateMac returned HTTP %d", resp.StatusCode)
	}
	var result awsKMSGenerateMACResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return "", fmt.Errorf("decode AWS KMS response: %w", err)
	}
	if strings.TrimSpace(result.KeyID) == "" {
		return "", fmt.Errorf("AWS KMS returned empty key id")
	}
	if result.MACAlgorithm != "HMAC_SHA_256" {
		return "", fmt.Errorf("AWS KMS MAC algorithm mismatch: got %q", result.MACAlgorithm)
	}
	mac, err := base64.StdEncoding.DecodeString(result.MAC)
	if err != nil || len(mac) != sha256.Size {
		return "", fmt.Errorf("AWS KMS returned invalid HMAC")
	}
	return "base64=" + base64.StdEncoding.EncodeToString(mac), nil
}

func (s *AWSKMSMACSigner) signV4(req *http.Request, body []byte, now time.Time, credentials AWSCredentials) error {
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	if token := strings.TrimSpace(credentials.SessionToken); token != "" {
		req.Header.Set("X-Amz-Security-Token", token)
	}
	payloadHash := sha256.Sum256(body)
	canonicalHeaders := map[string]string{
		"content-type": strings.TrimSpace(req.Header.Get("Content-Type")),
		"host":         req.URL.Host,
		"x-amz-date":   amzDate,
		"x-amz-target": strings.TrimSpace(req.Header.Get("X-Amz-Target")),
	}
	if token := strings.TrimSpace(credentials.SessionToken); token != "" {
		canonicalHeaders["x-amz-security-token"] = token
	}
	names := make([]string, 0, len(canonicalHeaders))
	for name := range canonicalHeaders {
		names = append(names, name)
	}
	sort.Strings(names)
	var headerBlock strings.Builder
	for _, name := range names {
		headerBlock.WriteString(name)
		headerBlock.WriteByte(':')
		headerBlock.WriteString(strings.Join(strings.Fields(canonicalHeaders[name]), " "))
		headerBlock.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{
		req.Method,
		"/",
		"",
		headerBlock.String(),
		signedHeaders,
		hex.EncodeToString(payloadHash[:]),
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	scope := date + "/" + s.config.Region + "/kms/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	kDate := hmacSHA256([]byte("AWS4"+credentials.SecretAccessKey), date)
	kRegion := hmacSHA256(kDate, s.config.Region)
	kService := hmacSHA256(kRegion, "kms")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+credentials.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
	return nil
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}
