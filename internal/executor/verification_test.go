package executor

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/verification"
)

func TestVerificationArchivePAXMetadataAndConfinement(t *testing.T) {
	archive := func(headers ...*tar.Header) []byte {
		t.Helper()
		var b bytes.Buffer
		tw := tar.NewWriter(&b)
		if err := tw.WriteHeader(&tar.Header{Name: "pax_global_header", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "0123456789012345678901234567890123456789"}}); err != nil {
			t.Fatal(err)
		}
		for _, h := range headers {
			if err := tw.WriteHeader(h); err != nil {
				t.Fatal(err)
			}
			if h.Size > 0 {
				if _, err := tw.Write(bytes.Repeat([]byte{'x'}, int(h.Size))); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	dest := t.TempDir()
	data := archive(&tar.Header{Name: "document.txt", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg})
	if err := extractVerificationArchive(bytes.NewReader(data), dest); err != nil {
		t.Fatal("legal Git PAX archive rejected", err)
	}
	if b, err := os.ReadFile(filepath.Join(dest, "document.txt")); err != nil || string(b) != "x" {
		t.Fatalf("file missing: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(dest, "pax_global_header")); !os.IsNotExist(err) {
		t.Fatal("PAX metadata written as file", err)
	}
	for _, h := range []*tar.Header{
		{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o600},
		{Name: "/absolute", Typeflag: tar.TypeReg, Mode: 0o600},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../escape"},
		{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "../escape"},
		{Name: "../" + strings.Repeat("pax-escape", 20), Typeflag: tar.TypeReg, Mode: 0o600, Format: tar.FormatPAX},
	} {
		t.Run(h.Name, func(t *testing.T) {
			if err := extractVerificationArchive(bytes.NewReader(archive(h)), t.TempDir()); err == nil {
				t.Fatalf("accepted escaping/link entry: %+v", h)
			}
		})
	}
}

type recordVerification struct{ reports int }

func (r *recordVerification) BeginVerificationAttempt(context.Context, *domain.Task, *domain.VerificationReport) error {
	r.reports++
	return nil
}
func TestVerificationNeverFallsBackToDemo(t *testing.T) {
	r := NewRouter(NewDemoExecutor())
	out, err := r.ExecuteDetailed(context.Background(), &domain.Task{Type: domain.VerificationTaskType})
	if err == nil || IsRetryable(err) || strings.Contains(out.Result, "processed") {
		t.Fatalf("fallback accepted: %+v %v", out, err)
	}
}
func TestVerificationRunsExactCommitAndKeepsFailures(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("trusted verification runtime is Linux")
	}
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init")
	git("config", "user.name", "verification-test")
	git("config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(repo, "value.txt"), []byte("committed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "value.txt")
	git("commit", "-m", "fixture")
	commit := git("rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repo, "value.txt"), []byte("uncommitted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cat, err := exec.LookPath("cat")
	if err != nil {
		t.Fatal(err)
	}
	falseBin, err := exec.LookPath("false")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &verification.Config{WorkRoot: filepath.Join(root, "work"), ArtifactRoot: filepath.Join(root, "reports"), Projects: []verification.Project{{ID: "demo", Repository: repo, Subjects: []string{"svc"}, Profile: verification.Profile{ID: "verify-change", Revision: "1", Checks: []verification.Check{{ID: "committed", Required: true, Argv: []string{cat, "value.txt"}}, {ID: "assertion", Required: true, Argv: []string{falseBin}}}}}}}
	input := domain.VerificationRequest{RunID: "d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3", ProjectID: "demo", TargetCommit: commit, CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"committed", "assertion"}}
	payload, _ := json.Marshal(verification.TaskPayload{VerificationRequest: input, RegisteredProfile: cfg.Projects[0].Profile})
	// Admission captured the old profile; runtime config edits must not turn
	// that accepted failing plan into a passing command.
	cfg.Projects[0].Profile.Checks[1].Argv = []string{cat, "value.txt"}
	recorder := &recordVerification{}
	e := NewVerificationExecutor(cfg, recorder)
	out, err := e.ExecuteDetailed(context.Background(), &domain.Task{ID: 1, Type: domain.VerificationTaskType, Payload: string(payload), EnqueueGeneration: 1})
	if err == nil || IsRetryable(err) || out.Verification.Verdict != "failed" {
		t.Fatalf("lost failure: %+v %v", out.Verification, err)
	}
	if out.MetadataJSON != "" {
		t.Fatal("verification leaked to AI audit")
	}
	b, err := os.ReadFile(filepath.Join(cfg.ArtifactRoot, input.RunID, "1", "check-0.log"))
	if err != nil || string(b) != "committed\n" {
		t.Fatalf("not exact commit: %q %v", b, err)
	}
	b, _ = os.ReadFile(filepath.Join(repo, "value.txt"))
	if string(b) != "uncommitted\n" {
		t.Fatal("source directory modified")
	}
	if recorder.reports != 3 {
		t.Fatalf("expected initial+2 check reports: %d", recorder.reports)
	}
	cfg.Projects[0].Profile.Checks = cfg.Projects[0].Profile.Checks[:1]
	input.CheckIDs = []string{"committed"}
	input.RunID = "ec7c861d-b65c-4e78-8efa-511084919c6a"
	payload, _ = json.Marshal(verification.TaskPayload{VerificationRequest: input, RegisteredProfile: cfg.Projects[0].Profile})
	out, err = e.ExecuteDetailed(context.Background(), &domain.Task{ID: 2, Type: domain.VerificationTaskType, Payload: string(payload), EnqueueGeneration: 1})
	if err != nil || out.Verification.Verdict != "passed" {
		t.Fatalf("success failed: %+v %v", out, err)
	}
}
