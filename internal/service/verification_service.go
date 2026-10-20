package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/verification"
)

var ErrVerificationArtifactNotFound = errors.New("verification artifact not found")

type VerificationService struct {
	repo   *repository.TaskRepository
	config *verification.Config
}

func NewVerificationService(repo *repository.TaskRepository, cfg *verification.Config) *VerificationService {
	return &VerificationService{repo: repo, config: cfg}
}
func (s *VerificationService) Submit(ctx context.Context, subject string, in domain.VerificationRequest) (*domain.VerificationRunView, bool, error) {
	if s.config == nil {
		return nil, false, verification.ErrUnavailable
	}
	if err := s.config.Authorize(subject, in.ProjectID); err != nil {
		return nil, false, err
	}
	if !verification.ValidRunID(in.RunID) {
		return nil, false, fmt.Errorf("%w: invalid run UUID", verification.ErrInvalid)
	}
	// Recover accepted requests before validating against today's registry.
	// Advancing a profile revision must not break an old UUID's idempotency.
	if _, err := s.repo.VerificationRun(ctx, in.RunID); err == nil {
		run, _, err := s.repo.CreateVerification(ctx, in, verification.Profile{})
		if err != nil {
			return nil, false, err
		}
		view, err := s.repo.VerificationView(ctx, run.RunID)
		return view, false, err
	} else if !errors.Is(err, repository.ErrVerificationNotFound) {
		return nil, false, err
	}
	input, err := s.config.Normalize(in)
	if err != nil {
		return nil, false, err
	}
	project, _ := s.config.Project(input.ProjectID)
	cmd := exec.CommandContext(ctx, "git", "-C", project.Repository, "cat-file", "-e", input.TargetCommit+"^{commit}")
	cmd.Env = []string{"GIT_CONFIG_NOSYSTEM=1"}
	if output, err := cmd.CombinedOutput(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			return nil, false, fmt.Errorf("%w: inspect registered repository: %v", verification.ErrUnavailable, err)
		}
		return nil, false, fmt.Errorf("%w: target commit is not available in the registered repository: %v: %s", verification.ErrInvalid, err, string(output))
	}
	run, created, err := s.repo.CreateVerification(ctx, input, project.Profile)
	if err != nil {
		return nil, false, err
	}
	view, err := s.repo.VerificationView(ctx, run.RunID)
	return view, created, err
}
func (s *VerificationService) Get(ctx context.Context, subject, id string) (*domain.VerificationRunView, error) {
	if s.config == nil {
		return nil, verification.ErrUnavailable
	}
	if !verification.ValidRunID(id) {
		return nil, fmt.Errorf("%w: invalid run UUID", verification.ErrInvalid)
	}
	run, err := s.repo.VerificationRun(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.config.Authorize(subject, run.ProjectID); err != nil {
		return nil, err
	}
	return s.repo.VerificationView(ctx, id)
}
func (s *VerificationService) Artifact(ctx context.Context, subject, id string, generation uint64, artifact string) ([]byte, error) {
	view, err := s.Get(ctx, subject, id)
	if err != nil {
		return nil, err
	}
	for _, attempt := range view.Attempts {
		if attempt.Generation != generation {
			continue
		}
		for _, check := range attempt.Checks {
			if check.ArtifactID != artifact || artifact == "" {
				continue
			}
			// IDs come exclusively from the persisted report. The report's own
			// generation determines a confined run directory; request paths never do.
			if filepath.Base(artifact) != artifact {
				return nil, ErrVerificationArtifactNotFound
			}
			path := filepath.Join(s.config.ArtifactRoot, id, strconv.FormatUint(generation, 10), artifact+".log")
			b, err := os.ReadFile(path)
			if os.IsNotExist(err) {
				return nil, ErrVerificationArtifactNotFound
			}
			return b, err
		}
	}
	return nil, ErrVerificationArtifactNotFound
}
