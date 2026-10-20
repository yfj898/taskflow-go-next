package verification

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/yfj898/taskflow-go/internal/domain"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	root := t.TempDir()
	return &Config{WorkRoot: filepath.Join(root, "work"), ArtifactRoot: filepath.Join(root, "reports"), Projects: []Project{{ID: "demo", Repository: filepath.Join(root, "repo"), Subjects: []string{"svc"}, Profile: Profile{ID: "verify-change", Revision: "1", Checks: []Check{{ID: "backend", Required: true, Argv: []string{"go", "test", "./..."}}, {ID: "frontend", Required: true, Argv: []string{"npm", "test"}}, {ID: "build", Required: true, Argv: []string{"npm", "run", "build"}}}}}}}
}
func validRequest() domain.VerificationRequest {
	return domain.VerificationRequest{RunID: "d05fb8d9-655c-4c20-8b2d-b2594c5a4dc3", ProjectID: "demo", TargetCommit: "0123456789012345678901234567890123456789", CheckProfileID: "verify-change", CheckProfileRevision: "1", CheckIDs: []string{"build", "frontend", "backend"}, ExternalSnapshotID: "snapshot"}
}
func TestRegistrationAndInputBoundaries(t *testing.T) {
	c := testConfig(t)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := c.Authorize("svc", "demo"); err != nil {
		t.Fatal(err)
	}
	if err := c.Authorize("other", "demo"); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	if err := c.Authorize("svc", "other"); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	in := validRequest()
	got, err := c.Normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.CheckIDs[0] != "backend" {
		t.Fatal("not canonicalized")
	}
	for _, change := range []func(*domain.VerificationRequest){func(r *domain.VerificationRequest) { r.RunID = "../path" }, func(r *domain.VerificationRequest) { r.TargetCommit = "main" }, func(r *domain.VerificationRequest) { r.CheckIDs = []string{"backend"} }, func(r *domain.VerificationRequest) { r.CheckIDs = append(r.CheckIDs, "shell") }, func(r *domain.VerificationRequest) { r.CheckProfileRevision = "old" }} {
		bad := validRequest()
		change(&bad)
		if _, err := c.Normalize(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted %+v: %v", bad, err)
		}
	}
	c.Projects[0].Profile.Checks[0].Directory = "../outside"
	if err := c.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted traversal", err)
	}
}
