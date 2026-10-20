package executor

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/verification"
)

type VerificationRecorder interface {
	BeginVerificationAttempt(context.Context, *domain.Task, *domain.VerificationReport) error
}
type VerificationExecutor struct {
	config   *verification.Config
	recorder VerificationRecorder
}

func NewVerificationExecutor(cfg *verification.Config, recorder VerificationRecorder) *VerificationExecutor {
	return &VerificationExecutor{config: cfg, recorder: recorder}
}
func (e *VerificationExecutor) Execute(ctx context.Context, task *domain.Task) (string, error) {
	r, err := e.ExecuteDetailed(ctx, task)
	return r.Result, err
}
func (e *VerificationExecutor) ExecuteDetailed(ctx context.Context, task *domain.Task) (out ExecutionResult, retErr error) {
	report := &domain.VerificationReport{Generation: task.EnqueueGeneration, StartedAt: time.Now().UTC(), Checks: []domain.VerificationCheck{}}
	out.Verification = report
	defer func() {
		now := time.Now().UTC()
		report.FinishedAt = &now
		report.Verdict = domain.VerificationVerdict(report.Checks)
		if retErr != nil {
			report.Error = retErr.Error()
			if report.Verdict == "passed" {
				report.Verdict = "incomplete"
			}
		}
		out.Result = "verification " + report.Verdict
	}()
	var payload verification.TaskPayload
	if err := json.Unmarshal([]byte(task.Payload), &payload); err != nil {
		return out, Permanent(fmt.Errorf("decode verification task: %w", err))
	}
	project, err := e.config.Project(payload.ProjectID)
	if err != nil {
		return out, Permanent(err)
	}
	// A restart or a newly registered revision cannot silently change commands
	// of an accepted run. Repository/env remain operator-owned runtime mapping.
	registered := *project
	registered.Profile = payload.RegisteredProfile
	plan := &verification.Config{WorkRoot: e.config.WorkRoot, ArtifactRoot: e.config.ArtifactRoot, Projects: []verification.Project{registered}}
	if err := plan.Validate(); err != nil {
		return out, Permanent(fmt.Errorf("persisted check profile: %w", err))
	}
	input, err := plan.Normalize(payload.VerificationRequest)
	if err != nil {
		return out, Permanent(err)
	}
	project = &registered
	selected := map[string]bool{}
	for _, id := range input.CheckIDs {
		selected[id] = true
	}
	checks := []verification.Check{}
	for _, ch := range project.Profile.Checks {
		if selected[ch.ID] {
			checks = append(checks, ch)
			report.Checks = append(report.Checks, domain.VerificationCheck{ID: ch.ID, Required: ch.Required, Status: "not_run"})
		}
	}
	if err := e.persist(ctx, task, report); err != nil {
		return out, Permanent(fmt.Errorf("start verification report: %w", err))
	}
	base := filepath.Join(e.config.WorkRoot, input.RunID, strconv.FormatUint(task.EnqueueGeneration, 10))
	// Never reuse an attempt directory: a prior process may still have outputs.
	if err := os.MkdirAll(filepath.Dir(base), 0o700); err != nil {
		return out, Permanent(err)
	}
	if err := os.Mkdir(base, 0o700); err != nil {
		return out, Permanent(fmt.Errorf("create isolated attempt directory: %w", err))
	}
	source := filepath.Join(base, "source")
	home := filepath.Join(base, "home")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{source, home, tmp} {
		if err := os.Mkdir(d, 0o700); err != nil {
			return out, Permanent(err)
		}
	}
	env := []string{"HOME=" + home, "TMPDIR=" + tmp}
	for k, v := range project.Environment {
		env = append(env, k+"="+v)
	}
	if err := extractCommit(ctx, project.Repository, input.TargetCommit, source, env); err != nil {
		return out, Permanent(fmt.Errorf("extract exact commit %s: %w", input.TargetCommit, err))
	}
	artifactDir := filepath.Join(e.config.ArtifactRoot, input.RunID, strconv.FormatUint(task.EnqueueGeneration, 10))
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return out, Permanent(err)
	}
	for i, ch := range checks {
		if ctx.Err() != nil {
			return out, Permanent(ctx.Err())
		}
		id := "check-" + strconv.Itoa(i)
		log, err := os.OpenFile(filepath.Join(artifactDir, id+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return out, Permanent(err)
		}
		binary, err := verificationExecutable(ch.Argv[0], project.Environment["PATH"])
		if err != nil {
			_ = log.Close()
			return out, Permanent(fmt.Errorf("check %s executable: %w", ch.ID, err))
		}
		cmd := exec.CommandContext(ctx, binary, ch.Argv[1:]...)
		cmd.Dir = filepath.Join(source, ch.Directory)
		cmd.Env = env
		cmd.Stdout = log
		cmd.Stderr = log
		configureVerificationProcess(cmd)
		runErr := cmd.Run()
		closeErr := log.Close()
		check := &report.Checks[i]
		if closeErr != nil {
			return out, Permanent(fmt.Errorf("close check %s log: %w", ch.ID, closeErr))
		}
		check.ArtifactID = id
		if runErr == nil {
			code := 0
			check.ExitCode = &code
			check.Status = "passed"
		} else {
			check.Error = runErr.Error()
			var exit *exec.ExitError
			if ctx.Err() != nil {
				return out, Permanent(fmt.Errorf("check %s interrupted: %w", ch.ID, ctx.Err()))
			}
			if !errors.As(runErr, &exit) {
				return out, Permanent(fmt.Errorf("start check %s: %w", ch.ID, runErr))
			}
			code := exit.ExitCode()
			if code < 0 {
				return out, Permanent(fmt.Errorf("check %s terminated: %w", ch.ID, runErr))
			}
			check.ExitCode = &code
			check.Status = "failed"
		}
		if err := e.persist(ctx, task, report); err != nil {
			return out, Permanent(fmt.Errorf("persist check %s: %w", ch.ID, err))
		}
	}
	if domain.VerificationVerdict(report.Checks) == "failed" {
		for _, check := range report.Checks {
			if check.Required && check.Status == "failed" {
				return out, Permanent(fmt.Errorf("check %s exited with status %d", check.ID, *check.ExitCode))
			}
		}
	}
	return out, nil
}
func (e *VerificationExecutor) persist(ctx context.Context, task *domain.Task, report *domain.VerificationReport) error {
	if e.recorder == nil {
		return errors.New("verification report recorder is required")
	}
	return e.recorder.BeginVerificationAttempt(ctx, task, report)
}
func verificationExecutable(name, path string) (string, error) {
	if filepath.IsAbs(name) {
		return name, nil
	}
	if strings.ContainsAny(name, "/\\") {
		return "", errors.New("executable must be absolute or a PATH name")
	}
	for _, dir := range filepath.SplitList(path) {
		if !filepath.IsAbs(dir) {
			continue
		}
		candidate := filepath.Join(dir, name)
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s is not on the configured PATH", name)
}
func extractCommit(ctx context.Context, repo, commit, dest string, env []string) error {
	git, err := exec.LookPath("git")
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, git, "-C", repo, "archive", "--format=tar", commit)
	cmd.Env = env
	configureVerificationProcess(cmd)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	extractErr := extractVerificationArchive(pipe, dest)
	if extractErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if extractErr != nil {
		return extractErr
	}
	if waitErr != nil {
		return fmt.Errorf("git archive: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func extractVerificationArchive(source io.Reader, dest string) error {
	tr := tar.NewReader(source)
	var extractErr error
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			extractErr = err
			break
		}
		name := filepath.FromSlash(h.Name)
		clean := filepath.Clean(name)
		if strings.HasPrefix(h.Name, "/") || filepath.IsAbs(name) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			extractErr = errors.New("archive path escapes attempt directory")
			break
		}
		path := filepath.Join(dest, clean)
		switch h.Typeflag {
		case tar.TypeXGlobalHeader, tar.TypeXHeader:
			// Git archive emits a global PAX header containing the source commit.
			// Metadata has no filesystem object. tar.Reader applies per-file PAX
			// paths before our normal confinement checks; links remain rejected.
			continue
		case tar.TypeDir:
			extractErr = os.MkdirAll(path, 0o700)
		case tar.TypeReg, tar.TypeRegA:
			if extractErr = os.MkdirAll(filepath.Dir(path), 0o700); extractErr == nil {
				var f *os.File
				f, extractErr = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0o777)
				if extractErr == nil {
					_, extractErr = io.Copy(f, tr)
					closeErr := f.Close()
					if extractErr == nil {
						extractErr = closeErr
					}
				}
			}
		default:
			extractErr = fmt.Errorf("unsupported archive entry type %d for %s", h.Typeflag, h.Name)
		}
		if extractErr != nil {
			break
		}
	}
	return extractErr
}
