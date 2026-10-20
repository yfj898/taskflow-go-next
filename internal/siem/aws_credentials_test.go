package siem

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestAWSWebIdentityProviderCachesTemporaryCredentials(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("oidc-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("Action") != "AssumeRoleWithWebIdentity" || r.Form.Get("WebIdentityToken") != "oidc-token" {
			t.Fatalf("unexpected STS form: %v", r.Form)
		}
		expires := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
		_, _ = fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>TEMP-AKID</AccessKeyId><SecretAccessKey>TEMP-SECRET</SecretAccessKey><SessionToken>TEMP-TOKEN</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, expires)
	}))
	defer server.Close()

	provider, err := NewAWSWebIdentityProvider(AWSWebIdentityProviderConfig{
		TokenFile:       tokenFile,
		RoleARN:         "arn:aws:iam::123456789012:role/taskflow",
		RoleSessionName: "taskflow-test",
		STSEndpoint:     server.URL,
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		creds, err := provider.Retrieve(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if creds.AccessKeyID != "TEMP-AKID" || creds.SecretAccessKey != "TEMP-SECRET" || creds.SessionToken != "TEMP-TOKEN" || creds.Source != "web-identity" {
			t.Fatalf("credentials = %+v", creds)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("STS requests = %d, want 1", got)
	}
}

func TestAWSIMDSv2ProviderUsesTokenAndCachesCredentials(t *testing.T) {
	var tokenRequests atomic.Int32
	var metadataRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			tokenRequests.Add(1)
			_, _ = w.Write([]byte("imdsv2-token"))
		case r.Method == http.MethodGet && r.URL.Path == "/latest/meta-data/iam/security-credentials/":
			metadataRequests.Add(1)
			if r.Header.Get("X-aws-ec2-metadata-token") != "imdsv2-token" {
				t.Fatal("missing IMDSv2 token")
			}
			_, _ = w.Write([]byte("taskflow-role\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/latest/meta-data/iam/security-credentials/taskflow-role":
			metadataRequests.Add(1)
			expires := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
			_, _ = fmt.Fprintf(w, `{"AccessKeyId":"IMDS-AKID","SecretAccessKey":"IMDS-SECRET","Token":"IMDS-TOKEN","Expiration":%q}`, expires)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider, err := NewAWSIMDSv2Provider(AWSIMDSv2ProviderConfig{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		creds, err := provider.Retrieve(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if creds.AccessKeyID != "IMDS-AKID" || creds.SessionToken != "IMDS-TOKEN" || creds.Source != "imds-v2" {
			t.Fatalf("credentials = %+v", creds)
		}
	}
	if tokenRequests.Load() != 1 || metadataRequests.Load() != 2 {
		t.Fatalf("token requests=%d metadata requests=%d", tokenRequests.Load(), metadataRequests.Load())
	}
}
