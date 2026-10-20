package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

const DeadLetterArchiveFormatVersion = 1

var (
	ErrInvalidObjectKey = errors.New("invalid archive object key")
	ErrObjectNotFound   = errors.New("archive object not found")
	ErrListUnsupported  = errors.New("archive sink does not support listing")
)

// DeadLetterPackage is the immutable forensic payload stored outside the hot
// MySQL DLQ table. Replay audits are included as a point-in-time snapshot; the
// live audit table keeps its own Phase 19 retention lifecycle.
type DeadLetterPackage struct {
	FormatVersion int                            `json:"format_version"`
	ArchivedAt    time.Time                      `json:"archived_at"`
	DeadLetter    domain.DeadLetterMessage       `json:"dead_letter"`
	ReplayAudits  []domain.DeadLetterReplayAudit `json:"replay_audits"`
}

func MarshalDeadLetterPackage(message domain.DeadLetterMessage, audits []domain.DeadLetterReplayAudit, archivedAt time.Time) ([]byte, string, error) {
	pkg := DeadLetterPackage{
		FormatVersion: DeadLetterArchiveFormatVersion,
		ArchivedAt:    archivedAt.UTC(),
		DeadLetter:    message,
		ReplayAudits:  audits,
	}
	data, err := json.Marshal(pkg)
	if err != nil {
		return nil, "", err
	}
	return data, SHA256Hex(data), nil
}

func UnmarshalDeadLetterPackage(data []byte) (*DeadLetterPackage, error) {
	var pkg DeadLetterPackage
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, err
	}
	if pkg.FormatVersion != DeadLetterArchiveFormatVersion {
		return nil, fmt.Errorf("unsupported dead-letter archive format version %d", pkg.FormatVersion)
	}
	return &pkg, nil
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type Sink interface {
	Name() string
	Put(context.Context, string, []byte) error
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) error
}

type ObjectInfo struct {
	Key        string
	SizeBytes  int64
	ModifiedAt time.Time
}

// Lister is optional because not every object-store gateway exposes a
// vendor-neutral listing API. Archive orphan reconciliation uses it when the
// configured sink supports object enumeration.
type Lister interface {
	List(context.Context, string) ([]ObjectInfo, error)
}

type namedSink struct {
	name string
	Sink
}

func WithName(name string, sink Sink) Sink {
	name = strings.TrimSpace(name)
	if name == "" || sink == nil {
		return sink
	}
	return &namedSink{name: name, Sink: sink}
}

func (s *namedSink) Name() string { return s.name }

func (s *namedSink) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	lister, ok := s.Sink.(Lister)
	if !ok {
		return nil, ErrListUnsupported
	}
	return lister.List(ctx, prefix)
}

// FileSink is the durable local/shared-filesystem reference implementation of
// the archive sink contract. Production Kubernetes deployments should mount a
// durable RWX volume or select an external object-store adapter.
type FileSink struct {
	root string
}

func NewFileSink(root string) (*FileSink, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("archive directory is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return &FileSink{root: abs}, nil
}

func (s *FileSink) Name() string { return "file" }

func (s *FileSink) pathFor(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(key)))
	if clean == "." || clean == "" || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", ErrInvalidObjectKey
	}
	path := filepath.Join(s.root, clean)
	rel, err := filepath.Rel(s.root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", ErrInvalidObjectKey
	}
	return path, nil
}

func (s *FileSink) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.pathFor(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".archive-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (s *FileSink) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.pathFor(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	return data, err
}

func (s *FileSink) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.pathFor(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *FileSink) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	root := s.root
	if prefix != "" {
		path, err := s.pathFor(prefix)
		if err != nil {
			return nil, err
		}
		root = path
	}
	infos := make([]ObjectInfo, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		infos = append(infos, ObjectInfo{
			Key:        filepath.ToSlash(rel),
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime(),
		})
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return infos, nil
	}
	return infos, err
}

// HTTPObjectSink is a small vendor-neutral adapter for object-storage gateways
// exposing PUT/GET semantics. Authentication is optional bearer auth so the
// taskflow binary does not embed any cloud-vendor SDK or credential model.
type HTTPObjectSink struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

func NewHTTPObjectSink(baseURL, bearerToken string, timeout time.Duration) (*HTTPObjectSink, error) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, errors.New("valid archive HTTP base URL is required")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("archive HTTP base URL must use http or https")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPObjectSink{baseURL: u, token: strings.TrimSpace(bearerToken), client: &http.Client{Timeout: timeout}}, nil
}

func (s *HTTPObjectSink) Name() string { return "http" }

func (s *HTTPObjectSink) objectURL(key string) (string, error) {
	key = strings.Trim(strings.TrimSpace(key), "/")
	if key == "" || key == "." || key == ".." || strings.Contains(key, "../") || strings.Contains(key, "/..") {
		return "", ErrInvalidObjectKey
	}
	parts := strings.Split(key, "/")
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", ErrInvalidObjectKey
		}
		parts[i] = url.PathEscape(part)
	}
	u := *s.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.Join(parts, "/")
	return u.String(), nil
}

func (s *HTTPObjectSink) do(ctx context.Context, method, key string, body io.Reader) (*http.Response, error) {
	objectURL, err := s.objectURL(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, objectURL, body)
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}
	return s.client.Do(req)
}

func (s *HTTPObjectSink) Put(ctx context.Context, key string, data []byte) error {
	resp, err := s.do(ctx, http.MethodPut, key, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("archive HTTP PUT returned %s", resp.Status)
	}
	return nil
}

func (s *HTTPObjectSink) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("archive HTTP GET returned %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}

func (s *HTTPObjectSink) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("archive HTTP DELETE returned %s", resp.Status)
	}
	return nil
}
