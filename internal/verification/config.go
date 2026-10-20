package verification

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yfj898/taskflow-go/internal/domain"
)

var ErrInvalid = errors.New("invalid verification input")
var ErrDenied = errors.New("verification project access denied")
var ErrUnavailable = errors.New("verification unavailable")

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var commitID = regexp.MustCompile(`^[0-9a-f]{40}$`)
var runUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Check struct {
	ID        string   `json:"id"`
	Required  bool     `json:"required"`
	Argv      []string `json:"argv"`
	Directory string   `json:"directory"`
}
type Profile struct {
	ID       string  `json:"id"`
	Revision string  `json:"revision"`
	Checks   []Check `json:"checks"`
}
type Project struct {
	ID          string            `json:"id"`
	Repository  string            `json:"repository"`
	Subjects    []string          `json:"subjects"`
	Profile     Profile           `json:"profile"`
	Environment map[string]string `json:"environment"`
}
type Config struct {
	WorkRoot     string    `json:"work_root"`
	ArtifactRoot string    `json:"artifact_root"`
	Projects     []Project `json:"projects"`
}

// TaskPayload captures the server's registered profile at admission. It is
// written only by VerificationService, never copied from the HTTP body.
type TaskPayload struct {
	domain.VerificationRequest
	RegisteredProfile Profile `json:"registered_check_profile"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("verification config: %w", err)
	}
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
func (c *Config) Validate() error {
	if !filepath.IsAbs(c.WorkRoot) || !filepath.IsAbs(c.ArtifactRoot) || filepath.Clean(c.WorkRoot) == filepath.Clean(c.ArtifactRoot) {
		return fmt.Errorf("%w: distinct absolute work/artifact roots required", ErrInvalid)
	}
	seen := map[string]bool{}
	if len(c.Projects) == 0 {
		return fmt.Errorf("%w: no registered projects", ErrInvalid)
	}
	for _, p := range c.Projects {
		if !identifier.MatchString(p.ID) || seen[p.ID] || !filepath.IsAbs(p.Repository) || len(p.Subjects) == 0 {
			return fmt.Errorf("%w: invalid project %q", ErrInvalid, p.ID)
		}
		seen[p.ID] = true
		if p.Profile.ID != "verify-change" || !identifier.MatchString(p.Profile.Revision) || len(p.Profile.Checks) == 0 {
			return fmt.Errorf("%w: invalid profile for %s", ErrInvalid, p.ID)
		}
		checks := map[string]bool{}
		required := false
		for _, ch := range p.Profile.Checks {
			if !identifier.MatchString(ch.ID) || checks[ch.ID] || len(ch.Argv) == 0 || strings.TrimSpace(ch.Argv[0]) == "" || filepath.IsAbs(ch.Directory) || ch.Directory == ".." || strings.HasPrefix(filepath.Clean(ch.Directory), ".."+string(filepath.Separator)) {
				return fmt.Errorf("%w: invalid check %q", ErrInvalid, ch.ID)
			}
			checks[ch.ID] = true
			required = required || ch.Required
		}
		if !required {
			return fmt.Errorf("%w: profile requires at least one mandatory check", ErrInvalid)
		}
		for _, s := range p.Subjects {
			if strings.TrimSpace(s) == "" {
				return fmt.Errorf("%w: empty subject", ErrInvalid)
			}
		}
		for k := range p.Environment {
			if k == "" || strings.ContainsAny(k, "=\x00") || k == "HOME" || k == "TMPDIR" || k == "SSH_AUTH_SOCK" {
				return fmt.Errorf("%w: forbidden environment key %q", ErrInvalid, k)
			}
		}
	}
	return nil
}
func ValidRunID(s string) bool { return runUUID.MatchString(s) }
func (c *Config) Project(id string) (*Project, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	for i := range c.Projects {
		if c.Projects[i].ID == id {
			return &c.Projects[i], nil
		}
	}
	return nil, fmt.Errorf("%w: unregistered project", ErrInvalid)
}
func (c *Config) Authorize(subject, project string) error {
	p, err := c.Project(project)
	if err != nil {
		return ErrDenied
	}
	for _, s := range p.Subjects {
		if s == subject && subject != "" {
			return nil
		}
	}
	return ErrDenied
}
func (c *Config) Normalize(in domain.VerificationRequest) (domain.VerificationRequest, error) {
	if !ValidRunID(in.RunID) || !commitID.MatchString(in.TargetCommit) || len(in.ExternalSnapshotID) > 255 {
		return in, fmt.Errorf("%w: UUID, full lowercase commit and valid snapshot required", ErrInvalid)
	}
	p, err := c.Project(in.ProjectID)
	if err != nil {
		return in, err
	}
	if in.CheckProfileID != p.Profile.ID || in.CheckProfileRevision != p.Profile.Revision {
		return in, fmt.Errorf("%w: unsupported profile revision", ErrInvalid)
	}
	wanted := map[string]bool{}
	for _, id := range in.CheckIDs {
		if wanted[id] {
			return in, fmt.Errorf("%w: duplicate check", ErrInvalid)
		}
		wanted[id] = true
	}
	ids := []string{}
	for _, ch := range p.Profile.Checks {
		if ch.Required && !wanted[ch.ID] {
			return in, fmt.Errorf("%w: required check %s missing", ErrInvalid, ch.ID)
		}
		if wanted[ch.ID] {
			ids = append(ids, ch.ID)
			delete(wanted, ch.ID)
		}
	}
	if len(wanted) > 0 {
		return in, fmt.Errorf("%w: unregistered check", ErrInvalid)
	}
	in.CheckIDs = ids
	return in, nil
}
