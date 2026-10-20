package cloudclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVerificationClientContracts(t *testing.T) {
	id := "d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3"
	posts := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-only" {
			t.Error("missing auth")
		}
		if strings.Contains(r.URL.Path, "/artifacts/") {
			if r.URL.Query().Get("generation") != "2" {
				t.Error("wrong generation")
			}
			_, _ = io.WriteString(w, "actual failure log\n")
			return
		}
		if r.Method == http.MethodPost {
			posts++
			if r.Header.Get("Idempotency-Key") != id {
				t.Error("unstable UUID")
			}
			var in VerificationRequest
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.RunID != id {
				t.Error("bad request", err)
			}
			w.WriteHeader(http.StatusAccepted)
		}
		_ = json.NewEncoder(w).Encode(VerificationRun{VerificationRequest: VerificationRequest{RunID: id}, TaskID: 12, Status: "failed", Generation: 2, Attempts: []VerificationReport{{Generation: 2, Verdict: "failed", Checks: []VerificationCheck{{ID: "backend", Status: "failed"}}}}})
	}))
	defer s.Close()
	c, err := New(s.URL, func(context.Context) (string, error) { return "test-only", nil }, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.SubmitVerification(context.Background(), VerificationRequest{RunID: id})
	if err != nil || out.TaskID != 12 || posts != 1 {
		t.Fatalf("submit: %+v %v", out, err)
	}
	out, err = c.GetVerificationRun(context.Background(), id)
	if err != nil || out.Attempts[0].Verdict != "failed" {
		t.Fatalf("query: %+v %v", out, err)
	}
	b, err := c.GetVerificationArtifact(context.Background(), id, 2, "check-0")
	if err != nil || string(b) != "actual failure log\n" {
		t.Fatalf("artifact: %q %v", b, err)
	}
	if _, err = c.SubmitVerification(context.Background(), VerificationRequest{}); err == nil || posts != 1 {
		t.Fatal("empty UUID sent mutation")
	}
}
func TestVerificationMutationIsNotRetried(t *testing.T) {
	posts := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"queue unavailable"}`)
	}))
	defer s.Close()
	c, _ := New(s.URL, func(context.Context) (string, error) { return "test", nil }, s.Client())
	_, err := c.SubmitVerification(context.Background(), VerificationRequest{RunID: "stable"})
	if err == nil || posts != 1 {
		t.Fatalf("mutation retry: posts=%d err=%v", posts, err)
	}
}
