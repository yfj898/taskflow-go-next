package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCLIReadAndMutationThroughHTTP(t *testing.T) {
	t.Setenv("TASKFLOW_ACCESS_TOKEN", "test-signed-access-token")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-signed-access-token" {
			t.Errorf("auth: %s", got)
		}
		if r.Method == http.MethodPost {
			if r.Header.Get("Idempotency-Key") != "business-order-12" {
				t.Error("idempotency key missing")
			}
			if r.URL.Path != "/api/v1/cloud/workflows/provision" {
				t.Errorf("wrong endpoint: %s", r.URL.Path)
			}
			w.WriteHeader(202)
			_, _ = w.Write([]byte(`{"workflow":{"id":31,"status":"running"},"order":{"id":1},"resource":{"id":2}}`))
			return
		}
		if r.URL.Path != "/api/v1/cloud/tenants/team-a/quota" {
			t.Errorf("wrong tenant path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"tenant_id":"team-a","replica_limit":3}`))
	}))
	defer srv.Close()
	var out, errs bytes.Buffer
	err := run([]string{"-endpoint", srv.URL, "quota", "team-a"}, &out, &errs)
	if err != nil {
		t.Fatalf("read: %v; stderr=%s", err, &errs)
	}
	var q map[string]any
	if err := json.Unmarshal(out.Bytes(), &q); err != nil || q["tenant_id"] != "team-a" {
		t.Fatalf("quota output=%s err=%v", out.String(), err)
	}
	file := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(file, []byte(`{"compute":{"namespace":"default","image":"nginx:1.27","replicas":1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	err = run([]string{"-endpoint", srv.URL, "provision", "team-a", "app-one", file, "-key=business-order-12"}, &out, &errs)
	if err != nil {
		t.Fatalf("provision: %v; stderr=%s", err, &errs)
	}
	var state map[string]any
	if err := json.Unmarshal(out.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	workflow := state["workflow"].(map[string]any)
	if workflow["id"] != float64(31) {
		t.Fatalf("unexpected output=%s", out.String())
	}
}

func TestCLIInvalidInputsBlocked(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "ftp://127.0.0.1:80", "http://user:pass@localhost:90", "http://10.0.0.7:90"} {
		if err := validateEndpoint(endpoint); err == nil {
			t.Errorf("unsafe endpoint accepted: %s", endpoint)
		}
	}
	var out, errs bytes.Buffer
	t.Setenv("TASKFLOW_ACCESS_TOKEN", "opaque")
	tests := [][]string{
		{"provision", "team-a", "name", "missing.json"},
		{"delete", "42"},
		{"quota-set", "team-a", "-4", "10", "3", "-key=order"},
		{"resources", "team-a", "101"},
		{"resource", "0"},
		{"provision", "team-a", "name", "missing.json", "-key= a"},
	}
	for _, args := range tests {
		err := run(args, &out, &errs)
		if err == nil {
			t.Errorf("invalid input accepted: %v", args)
		}
	}
}

func TestCLITokenFileRequires0600(t *testing.T) {
	f := filepath.Join(t.TempDir(), "access-token")
	if err := os.WriteFile(f, []byte("abc\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		t.Setenv("TASKFLOW_ACCESS_TOKEN", "must-not-fallback")
		got, err := tokenSource(f)(nil)
		if err == nil || got != "" || !strings.Contains(err.Error(), "TASKFLOW_ACCESS_TOKEN") {
			t.Fatalf("Windows token file must be rejected with actionable guidance: %q %v", got, err)
		}
		return
	}
	if _, err := tokenSource(f)(nil); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Errorf("file mode should fail: %v", err)
	}
	if err := os.Chmod(f, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := tokenSource(f)(nil); err != nil || got != "abc" {
		t.Errorf("private token file: %q %v", got, err)
	}
}
