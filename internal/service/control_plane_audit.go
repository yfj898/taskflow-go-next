package service

import (
	"context"
	"time"

	"github.com/yfj898/taskflow-go/internal/domain"
)

type ControlPlaneAuditRepository interface {
	CreateControlPlaneAudit(context.Context, *domain.ControlPlaneAudit) error
	CompleteControlPlaneAudit(context.Context, uint64, int, string, time.Time) error
	ListControlPlaneAudits(context.Context, int, int) ([]domain.ControlPlaneAudit, error)
	ListControlPlaneAuditsAfterSequence(context.Context, uint64, int) ([]domain.ControlPlaneAudit, error)
	GetControlPlaneAuditCheckpoint(context.Context) (domain.ControlPlaneAuditChainState, error)
	SealUnchainedControlPlaneAudits(context.Context, int) (int, error)
	VerifyControlPlaneAuditChain(context.Context) (domain.ControlPlaneAuditIntegrityReport, error)
	PurgeControlPlaneAuditsBefore(context.Context, time.Time, int) (int, error)
	ListControlPlaneAuditDeliveries(context.Context, string, int) ([]domain.ControlPlaneAuditDelivery, error)
	ListControlPlaneAuditDeliveryReceipts(context.Context, uint64, int) ([]domain.ControlPlaneAuditDeliveryReceipt, error)
	ListControlPlaneAuditDeliveryDLQ(context.Context, int) ([]domain.ControlPlaneAuditDeliveryDLQ, error)
	ReplayControlPlaneAuditDeliveryDLQ(context.Context, uint64, time.Time) error
	ControlPlaneAuditDeliveryStats(context.Context) (domain.ControlPlaneAuditDeliveryStats, error)
}

type ControlPlaneAuditService struct {
	repo ControlPlaneAuditRepository
}

func NewControlPlaneAuditService(repo ControlPlaneAuditRepository) *ControlPlaneAuditService {
	return &ControlPlaneAuditService{repo: repo}
}

func (s *ControlPlaneAuditService) Begin(ctx context.Context, audit *domain.ControlPlaneAudit) error {
	return s.repo.CreateControlPlaneAudit(ctx, audit)
}

func (s *ControlPlaneAuditService) Complete(ctx context.Context, id uint64, statusCode int, outcome string) error {
	return s.repo.CompleteControlPlaneAudit(ctx, id, statusCode, outcome, time.Now())
}

func (s *ControlPlaneAuditService) List(ctx context.Context, limit, offset int) ([]domain.ControlPlaneAudit, error) {
	return s.repo.ListControlPlaneAudits(ctx, limit, offset)
}

func (s *ControlPlaneAuditService) Export(ctx context.Context, after uint64, limit int) ([]domain.ControlPlaneAudit, domain.ControlPlaneAuditChainState, error) {
	audits, err := s.repo.ListControlPlaneAuditsAfterSequence(ctx, after, limit)
	if err != nil {
		return nil, domain.ControlPlaneAuditChainState{}, err
	}
	checkpoint, err := s.repo.GetControlPlaneAuditCheckpoint(ctx)
	return audits, checkpoint, err
}

func (s *ControlPlaneAuditService) Verify(ctx context.Context) (domain.ControlPlaneAuditIntegrityReport, error) {
	return s.repo.VerifyControlPlaneAuditChain(ctx)
}

func (s *ControlPlaneAuditService) SealLegacy(ctx context.Context, batch int) (int, error) {
	return s.repo.SealUnchainedControlPlaneAudits(ctx, batch)
}

func (s *ControlPlaneAuditService) PurgeBefore(ctx context.Context, cutoff time.Time, batch int) (int, error) {
	return s.repo.PurgeControlPlaneAuditsBefore(ctx, cutoff, batch)
}

func (s *ControlPlaneAuditService) ListDeliveries(ctx context.Context, state string, limit int) ([]domain.ControlPlaneAuditDelivery, error) {
	return s.repo.ListControlPlaneAuditDeliveries(ctx, state, limit)
}

func (s *ControlPlaneAuditService) ListDeliveryReceipts(ctx context.Context, afterSequence uint64, limit int) ([]domain.ControlPlaneAuditDeliveryReceipt, error) {
	return s.repo.ListControlPlaneAuditDeliveryReceipts(ctx, afterSequence, limit)
}

func (s *ControlPlaneAuditService) ListDeliveryDLQ(ctx context.Context, limit int) ([]domain.ControlPlaneAuditDeliveryDLQ, error) {
	return s.repo.ListControlPlaneAuditDeliveryDLQ(ctx, limit)
}

func (s *ControlPlaneAuditService) ReplayDeliveryDLQ(ctx context.Context, id uint64) error {
	return s.repo.ReplayControlPlaneAuditDeliveryDLQ(ctx, id, time.Now())
}

func (s *ControlPlaneAuditService) DeliveryStats(ctx context.Context) (domain.ControlPlaneAuditDeliveryStats, error) {
	return s.repo.ControlPlaneAuditDeliveryStats(ctx)
}
