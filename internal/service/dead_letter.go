package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	archivepkg "github.com/yfj898/taskflow-go/internal/archive"
	"github.com/yfj898/taskflow-go/internal/domain"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/repository"
)

var (
	ErrDeadLetterReplayUnavailable = errors.New("dead letter replay unavailable")
	ErrDeadLetterReplayStale       = errors.New("dead letter replay does not match current pending task generation")
	ErrDeadLetterArchiveDisabled   = errors.New("dead letter archive is disabled")
	ErrDeadLetterArchiveIntegrity  = errors.New("dead letter archive integrity check failed")
	ErrDeadLetterArchiveSink       = errors.New("dead letter archive sink unavailable")
	ErrDeadLetterArchiveCopies     = errors.New("dead letter archive does not have enough verified copies")
)

type ReplayDeadLetterInput struct {
	TaskID            uint64 `json:"task_id"`
	EnqueueGeneration uint64 `json:"enqueue_generation"`
}

type DeadLetterService struct {
	repo             *repository.TaskRepository
	queue            queue.TaskQueue
	replayLease      time.Duration
	enqueueLease     time.Duration
	archiveSink      archivepkg.Sink
	archiveReplicas  []archivepkg.Sink
	archiveMinCopies int
	archiveLease     time.Duration
}

func NewDeadLetterService(repo *repository.TaskRepository, q queue.TaskQueue, replayLease, enqueueLease time.Duration) *DeadLetterService {
	if replayLease <= 0 {
		replayLease = 30 * time.Second
	}
	if enqueueLease <= 0 {
		enqueueLease = 10 * time.Second
	}
	return &DeadLetterService{repo: repo, queue: q, replayLease: replayLease, enqueueLease: enqueueLease}
}

func (s *DeadLetterService) ConfigureArchive(sink archivepkg.Sink, lease time.Duration) {
	s.ConfigureArchiveReplication(sink, nil, lease, 1)
}

func (s *DeadLetterService) ConfigureArchiveReplication(sink archivepkg.Sink, replicas []archivepkg.Sink, lease time.Duration, minCopies int) {
	s.archiveSink = sink
	s.archiveReplicas = append([]archivepkg.Sink(nil), replicas...)
	if minCopies <= 0 {
		minCopies = 1
	}
	s.archiveMinCopies = minCopies
	if lease <= 0 {
		lease = 30 * time.Second
	}
	s.archiveLease = lease
}

func (s *DeadLetterService) Get(ctx context.Context, id uint64) (*domain.DeadLetterMessage, error) {
	return s.repo.GetDeadLetter(ctx, id)
}

func (s *DeadLetterService) List(ctx context.Context, limit, offset int) ([]domain.DeadLetterMessage, error) {
	return s.repo.ListDeadLetters(ctx, limit, offset)
}

func (s *DeadLetterService) ListReplayAudits(ctx context.Context, deadLetterID uint64, limit, offset int) ([]domain.DeadLetterReplayAudit, error) {
	return s.repo.ListDeadLetterReplayAudits(ctx, deadLetterID, limit, offset)
}

func (s *DeadLetterService) Purge(ctx context.Context, id uint64) error {
	if _, err := s.Archive(ctx, id); err != nil {
		return err
	}
	if err := s.repo.PurgeDeadLetter(ctx, id, s.archiveMinCopies); err != nil {
		return err
	}
	appmetrics.QueueDeadLetterPurged.WithLabelValues("manual").Inc()
	return nil
}

func archiveLeaseToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func (s *DeadLetterService) Archive(ctx context.Context, id uint64) (*domain.DeadLetterArchive, error) {
	if s.archiveSink == nil {
		appmetrics.QueueDeadLetterArchives.WithLabelValues("disabled").Inc()
		return nil, ErrDeadLetterArchiveDisabled
	}
	token, err := archiveLeaseToken()
	if err != nil {
		appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		return nil, err
	}
	now := time.Now()
	claim, err := s.repo.ClaimDeadLetterArchive(ctx, id, s.archiveSink.Name(), token, now, now.Add(s.archiveLease))
	if err != nil {
		if errors.Is(err, repository.ErrDeadLetterArchiveActive) || errors.Is(err, repository.ErrDeadLetterReplayActive) {
			appmetrics.QueueDeadLetterArchives.WithLabelValues("protected").Inc()
		} else {
			appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		}
		return nil, err
	}
	if claim.Ready {
		data, err := s.verifyArchiveObject(ctx, &claim.Archive)
		if err != nil {
			appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
			return nil, err
		}
		verifiedAt, copies, err := s.ensureArchiveCopies(ctx, &claim.Archive, data)
		if err != nil {
			appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
			return nil, err
		}
		claim.Archive.VerifiedAt = &verifiedAt
		claim.Archive.VerifiedCopies = copies
		appmetrics.QueueDeadLetterArchives.WithLabelValues("reused").Inc()
		return &claim.Archive, nil
	}

	data, digest, err := archivepkg.MarshalDeadLetterPackage(claim.Message, claim.ReplayAudits, now)
	if err != nil {
		_ = s.failArchive(claim, err)
		appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		return nil, err
	}
	objectKey := fmt.Sprintf("dead-letters/v1/%d/%s.json", claim.DeadLetterID, digest)
	if err := s.archiveSink.Put(ctx, objectKey, data); err != nil {
		_ = s.failArchive(claim, err)
		appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		return nil, err
	}
	readBack, err := s.archiveSink.Get(ctx, objectKey)
	if err != nil || int64(len(readBack)) != int64(len(data)) || archivepkg.SHA256Hex(readBack) != digest {
		if err == nil {
			err = ErrDeadLetterArchiveIntegrity
			appmetrics.QueueDeadLetterArchiveIntegrityFailures.Inc()
		}
		_ = s.failArchive(claim, err)
		appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		return nil, err
	}
	if err := s.repo.ConfirmDeadLetterArchive(ctx, claim, objectKey, digest, int64(len(data)), archivepkg.DeadLetterArchiveFormatVersion, now); err != nil {
		_ = s.failArchive(claim, err)
		appmetrics.QueueDeadLetterArchives.WithLabelValues("error").Inc()
		return nil, err
	}
	appmetrics.QueueDeadLetterArchives.WithLabelValues("success").Inc()
	appmetrics.QueueDeadLetterArchiveBytes.Add(float64(len(data)))
	archive, err := s.repo.GetDeadLetterArchiveByDeadLetterID(ctx, id)
	if err != nil {
		return nil, err
	}
	verifiedAt, copies, err := s.ensureArchiveCopies(ctx, archive, data)
	if err != nil {
		return nil, err
	}
	archive.VerifiedAt = &verifiedAt
	archive.VerifiedCopies = copies
	return archive, nil
}

func (s *DeadLetterService) ensureArchiveCopies(ctx context.Context, archive *domain.DeadLetterArchive, data []byte) (time.Time, int, error) {
	verifiedAt := time.Now()
	copies := 1
	for _, replica := range s.archiveReplicas {
		if replica == nil {
			continue
		}
		if err := replica.Put(ctx, archive.ObjectKey, data); err != nil {
			appmetrics.QueueDeadLetterArchiveReplications.WithLabelValues("error").Inc()
			continue
		}
		readBack, err := replica.Get(ctx, archive.ObjectKey)
		if err != nil || int64(len(readBack)) != archive.SizeBytes || archivepkg.SHA256Hex(readBack) != archive.SHA256 {
			appmetrics.QueueDeadLetterArchiveReplications.WithLabelValues("integrity_error").Inc()
			continue
		}
		receipt := &domain.DeadLetterArchiveReplica{
			ArchiveID:  archive.ID,
			Sink:       replica.Name(),
			ObjectKey:  archive.ObjectKey,
			SHA256:     archive.SHA256,
			SizeBytes:  archive.SizeBytes,
			VerifiedAt: verifiedAt,
		}
		if err := s.repo.UpsertDeadLetterArchiveReplica(ctx, receipt); err != nil {
			appmetrics.QueueDeadLetterArchiveReplications.WithLabelValues("error").Inc()
			continue
		}
		copies++
		appmetrics.QueueDeadLetterArchiveReplications.WithLabelValues("success").Inc()
	}
	if err := s.repo.MarkDeadLetterArchiveVerificationSummary(ctx, archive.ID, verifiedAt, copies); err != nil {
		return time.Time{}, copies, err
	}
	if copies < s.archiveMinCopies {
		appmetrics.QueueDeadLetterArchiveReplications.WithLabelValues("insufficient").Inc()
		return verifiedAt, copies, fmt.Errorf("%w: have %d, need %d", ErrDeadLetterArchiveCopies, copies, s.archiveMinCopies)
	}
	return verifiedAt, copies, nil
}

func (s *DeadLetterService) failArchive(claim repository.DeadLetterArchiveClaim, cause error) error {
	settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.repo.MarkDeadLetterArchiveFailed(settleCtx, claim, cause)
}

func (s *DeadLetterService) ListArchives(ctx context.Context, limit, offset int) ([]domain.DeadLetterArchive, error) {
	return s.repo.ListDeadLetterArchives(ctx, limit, offset)
}

func (s *DeadLetterService) GetArchive(ctx context.Context, id uint64) (*domain.DeadLetterArchive, error) {
	return s.repo.GetDeadLetterArchive(ctx, id)
}

func (s *DeadLetterService) archiveSinkByName(name string) archivepkg.Sink {
	if s.archiveSink != nil && s.archiveSink.Name() == name {
		return s.archiveSink
	}
	for _, replica := range s.archiveReplicas {
		if replica != nil && replica.Name() == name {
			return replica
		}
	}
	return nil
}

func (s *DeadLetterService) verifyArchiveObject(ctx context.Context, archive *domain.DeadLetterArchive) ([]byte, error) {
	if s.archiveSink == nil || archive.Sink != s.archiveSink.Name() {
		return nil, ErrDeadLetterArchiveSink
	}
	data, err := s.archiveSink.Get(ctx, archive.ObjectKey)
	if err != nil {
		if errors.Is(err, archivepkg.ErrObjectNotFound) {
			appmetrics.QueueDeadLetterArchiveIntegrityFailures.Inc()
			return nil, fmt.Errorf("%w: %v", ErrDeadLetterArchiveIntegrity, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrDeadLetterArchiveSink, err)
	}
	if int64(len(data)) != archive.SizeBytes || archivepkg.SHA256Hex(data) != archive.SHA256 {
		appmetrics.QueueDeadLetterArchiveIntegrityFailures.Inc()
		return nil, ErrDeadLetterArchiveIntegrity
	}
	return data, nil
}

func (s *DeadLetterService) readArchiveWithFallback(ctx context.Context, archive *domain.DeadLetterArchive) ([]byte, error) {
	data, err := s.verifyArchiveObject(ctx, archive)
	if err == nil {
		return data, nil
	}
	replicas, repoErr := s.repo.ListDeadLetterArchiveReplicas(ctx, archive.ID)
	if repoErr != nil {
		return nil, repoErr
	}
	for _, receipt := range replicas {
		sink := s.archiveSinkByName(receipt.Sink)
		if sink == nil {
			continue
		}
		candidate, getErr := sink.Get(ctx, receipt.ObjectKey)
		if getErr != nil || int64(len(candidate)) != receipt.SizeBytes || archivepkg.SHA256Hex(candidate) != receipt.SHA256 {
			continue
		}
		appmetrics.QueueDeadLetterArchiveReplicaReads.WithLabelValues("fallback").Inc()
		return candidate, nil
	}
	return nil, err
}

func (s *DeadLetterService) ExportArchive(ctx context.Context, id uint64) ([]byte, *domain.DeadLetterArchive, error) {
	archive, err := s.repo.GetDeadLetterArchive(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if archive.State != domain.DeadLetterArchiveReady {
		return nil, archive, repository.ErrDeadLetterArchiveActive
	}
	data, err := s.readArchiveWithFallback(ctx, archive)
	if err != nil {
		return nil, archive, err
	}
	return data, archive, nil
}

func (s *DeadLetterService) RestoreArchive(ctx context.Context, id uint64) (*domain.DeadLetterMessage, error) {
	data, archive, err := s.ExportArchive(ctx, id)
	if err != nil {
		appmetrics.QueueDeadLetterRestores.WithLabelValues("error").Inc()
		return nil, err
	}
	pkg, err := archivepkg.UnmarshalDeadLetterPackage(data)
	if err != nil {
		appmetrics.QueueDeadLetterArchiveIntegrityFailures.Inc()
		appmetrics.QueueDeadLetterRestores.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("%w: %v", ErrDeadLetterArchiveIntegrity, err)
	}
	if pkg.DeadLetter.ID != archive.DeadLetterID ||
		pkg.DeadLetter.SourceBackend != archive.SourceBackend ||
		pkg.DeadLetter.SourceQueue != archive.SourceQueue ||
		pkg.DeadLetter.SourceDeliveryID != archive.SourceDeliveryID {
		appmetrics.QueueDeadLetterArchiveIntegrityFailures.Inc()
		appmetrics.QueueDeadLetterRestores.WithLabelValues("error").Inc()
		return nil, ErrDeadLetterArchiveIntegrity
	}

	now := time.Now()
	restored := pkg.DeadLetter
	restored.ID = 0
	restored.ReplayState = domain.DeadLetterQuarantined
	restored.ReplayCount = 0
	restored.ReplayLeaseUntil = nil
	restored.LastReplayError = ""
	restored.ReplayedAt = nil
	restored.CreatedAt = now
	restored.UpdatedAt = now
	if err := s.repo.RestoreDeadLetterArchive(ctx, id, &restored, now); err != nil {
		if errors.Is(err, repository.ErrDeadLetterArchiveRestoreConflict) {
			appmetrics.QueueDeadLetterRestores.WithLabelValues("conflict").Inc()
		} else {
			appmetrics.QueueDeadLetterRestores.WithLabelValues("error").Inc()
		}
		return nil, err
	}
	appmetrics.QueueDeadLetterRestores.WithLabelValues("success").Inc()
	return &restored, nil
}

func (s *DeadLetterService) GCArchiveObject(ctx context.Context, id uint64, before time.Time) error {
	if err := s.repo.ClaimDeadLetterArchiveGC(ctx, id, before); err != nil {
		return err
	}
	archive, err := s.repo.GetDeadLetterArchive(ctx, id)
	if err != nil {
		return err
	}
	release := func(cause error) error {
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.repo.ReleaseDeadLetterArchiveGC(settleCtx, id, cause)
		return cause
	}

	replicas, err := s.repo.ListDeadLetterArchiveReplicas(ctx, id)
	if err != nil {
		return release(err)
	}
	for _, receipt := range replicas {
		sink := s.archiveSinkByName(receipt.Sink)
		if sink == nil {
			return release(fmt.Errorf("%w: replica sink %s", ErrDeadLetterArchiveSink, receipt.Sink))
		}
		if err := sink.Delete(ctx, receipt.ObjectKey); err != nil {
			return release(err)
		}
	}
	primary := s.archiveSinkByName(archive.Sink)
	if primary == nil {
		return release(ErrDeadLetterArchiveSink)
	}
	if err := primary.Delete(ctx, archive.ObjectKey); err != nil {
		return release(err)
	}
	if err := s.repo.MarkDeadLetterArchiveExpired(ctx, id, time.Now()); err != nil {
		return release(err)
	}
	appmetrics.QueueDeadLetterArchiveObjectGC.WithLabelValues("success").Inc()
	return nil
}

type ArchiveOrphanReconcileResult struct {
	Scanned int
	Deleted int
	Errors  int
}

func (s *DeadLetterService) ReconcileArchiveOrphans(ctx context.Context, grace time.Duration, limit int) ArchiveOrphanReconcileResult {
	if grace < 0 {
		grace = 0
	}
	if limit <= 0 {
		limit = 100
	}
	result := ArchiveOrphanReconcileResult{}
	cutoff := time.Now().Add(-grace)
	sinks := make([]archivepkg.Sink, 0, 1+len(s.archiveReplicas))
	if s.archiveSink != nil {
		sinks = append(sinks, s.archiveSink)
	}
	sinks = append(sinks, s.archiveReplicas...)
	for _, sink := range sinks {
		if sink == nil || result.Scanned >= limit {
			continue
		}
		lister, ok := sink.(archivepkg.Lister)
		if !ok {
			appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("unsupported").Inc()
			continue
		}
		infos, err := lister.List(ctx, "dead-letters/v1")
		if err != nil {
			if errors.Is(err, archivepkg.ErrListUnsupported) {
				appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("unsupported").Inc()
				continue
			}
			result.Errors++
			appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("error").Inc()
			continue
		}
		keys, err := s.repo.ListReferencedArchiveObjectKeys(ctx, sink.Name())
		if err != nil {
			result.Errors++
			appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("error").Inc()
			continue
		}
		known := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			known[key] = struct{}{}
		}
		for _, info := range infos {
			if result.Scanned >= limit {
				break
			}
			result.Scanned++
			if !info.ModifiedAt.Before(cutoff) {
				continue
			}
			if _, ok := known[info.Key]; ok {
				continue
			}
			if err := sink.Delete(ctx, info.Key); err != nil {
				result.Errors++
				appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("error").Inc()
				continue
			}
			result.Deleted++
			appmetrics.QueueDeadLetterArchiveOrphans.WithLabelValues("deleted").Inc()
		}
	}
	return result
}

func (s *DeadLetterService) DrillArchive(ctx context.Context, id uint64) error {
	archive, err := s.repo.GetDeadLetterArchive(ctx, id)
	if err != nil {
		return err
	}
	if archive.State != domain.DeadLetterArchiveReady {
		return repository.ErrDeadLetterArchiveActive
	}
	data, err := s.readArchiveWithFallback(ctx, archive)
	if err == nil {
		pkg, parseErr := archivepkg.UnmarshalDeadLetterPackage(data)
		if parseErr != nil || pkg.DeadLetter.ID != archive.DeadLetterID || pkg.DeadLetter.SourceDeliveryID != archive.SourceDeliveryID {
			if parseErr != nil {
				err = parseErr
			} else {
				err = ErrDeadLetterArchiveIntegrity
			}
		}
	}
	now := time.Now()
	_ = s.repo.MarkDeadLetterArchiveDrill(ctx, id, now, err)
	if err != nil {
		appmetrics.QueueDeadLetterArchiveDrills.WithLabelValues("error").Inc()
		return err
	}
	appmetrics.QueueDeadLetterArchiveDrills.WithLabelValues("success").Inc()
	appmetrics.QueueDeadLetterArchiveDrillLastSuccess.Set(float64(now.Unix()))
	return nil
}

func (s *DeadLetterService) recordReplayAudit(message *domain.DeadLetterMessage, taskID, generation uint64, outcome string, cause error) error {
	auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.repo.CreateDeadLetterReplayAudit(auditCtx, replayAuditSnapshot(message, taskID, generation, outcome, cause))
}

func replayAuditSnapshot(message *domain.DeadLetterMessage, taskID, generation uint64, outcome string, cause error) *domain.DeadLetterReplayAudit {
	audit := &domain.DeadLetterReplayAudit{
		DeadLetterID:      message.ID,
		SourceBackend:     message.SourceBackend,
		SourceQueue:       message.SourceQueue,
		SourceDeliveryID:  message.SourceDeliveryID,
		FailureClass:      message.FailureClass,
		TaskID:            taskID,
		EnqueueGeneration: generation,
		Outcome:           outcome,
	}
	if cause != nil {
		audit.FailureReason = cause.Error()
	}
	return audit
}

func (s *DeadLetterService) Replay(ctx context.Context, id uint64, in ReplayDeadLetterInput) (*domain.DeadLetterMessage, error) {
	message, err := s.repo.GetDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}
	taskID := in.TaskID
	if taskID == 0 && message.TaskID != nil {
		taskID = *message.TaskID
	}
	generation := in.EnqueueGeneration
	if generation == 0 && message.EnqueueGeneration != nil {
		generation = *message.EnqueueGeneration
	}

	reject := func(cause error) (*domain.DeadLetterMessage, error) {
		appmetrics.QueueDeadLetterReplays.WithLabelValues("rejected").Inc()
		if auditErr := s.recordReplayAudit(message, taskID, generation, "rejected", cause); auditErr != nil {
			return nil, auditErr
		}
		return nil, cause
	}
	recordError := func(cause error) {
		appmetrics.QueueDeadLetterReplays.WithLabelValues("error").Inc()
		_ = s.recordReplayAudit(message, taskID, generation, "error", cause)
	}

	if taskID == 0 || generation == 0 {
		return reject(fmt.Errorf("%w: task_id and enqueue_generation are required", ErrDeadLetterReplayUnavailable))
	}
	task, err := s.repo.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, repository.ErrTaskNotFound) {
			return reject(fmt.Errorf("%w: task not found", ErrDeadLetterReplayStale))
		}
		recordError(err)
		return nil, err
	}
	if task.Status != domain.TaskPending || task.EnqueueGeneration != generation {
		return reject(ErrDeadLetterReplayStale)
	}

	now := time.Now()
	claimed, err := s.repo.ClaimDeadLetterReplay(ctx, id, now, now.Add(s.replayLease))
	if err != nil {
		recordError(err)
		return nil, err
	}
	if !claimed {
		return reject(ErrDeadLetterReplayUnavailable)
	}

	enqueueClaim, enqueueState, err := s.repo.ClaimTaskEnqueue(ctx, taskID, generation, now, now.Add(s.enqueueLease))
	if err != nil {
		appmetrics.EnqueueFencing.WithLabelValues("dlq_replay", "error").Inc()
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.repo.MarkDeadLetterReplayFailed(settleCtx, id, err)
		cancel()
		recordError(err)
		return nil, err
	}
	appmetrics.EnqueueFencing.WithLabelValues("dlq_replay", string(enqueueState)).Inc()

	// Replay must acquire the same visibility lease as every automatic
	// publisher. In particular, already_visible is rejected rather than
	// guessing that the quarantined copy was the only broker-visible copy: an
	// operator replay must never manufacture an avoidable duplicate generation.
	if enqueueState != repository.EnqueueClaimAcquired {
		cause := ErrDeadLetterReplayUnavailable
		if enqueueState == repository.EnqueueClaimObsolete {
			cause = ErrDeadLetterReplayStale
		}
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.repo.MarkDeadLetterReplayFailed(settleCtx, id, cause)
		cancel()
		return reject(cause)
	}

	if err := s.queue.Push(ctx, queue.Message{TaskID: taskID, Generation: generation}); err != nil {
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.repo.ReleaseTaskEnqueue(settleCtx, enqueueClaim)
		_ = s.repo.MarkDeadLetterReplayFailed(settleCtx, id, err)
		cancel()
		recordError(err)
		return nil, err
	}

	settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = s.repo.ConfirmTaskEnqueuedAndDeadLetterReplayed(
		settleCtx,
		enqueueClaim,
		id,
		time.Now(),
		replayAuditSnapshot(message, taskID, generation, "success", nil),
	)
	cancel()
	if err != nil {
		recordError(err)
		return nil, err
	}
	appmetrics.EnqueueConfirmed.WithLabelValues("dlq_replay").Inc()
	appmetrics.QueueDeadLetterReplays.WithLabelValues("success").Inc()
	return s.repo.GetDeadLetter(ctx, id)
}
