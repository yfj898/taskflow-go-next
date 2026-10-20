package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/service"
)

type fakeControlPlaneAuditRepository struct {
	beginErr    error
	completeErr error
	created     []*domain.ControlPlaneAudit
	statusCode  int
	outcome     string
	completedID uint64
}

func (f *fakeControlPlaneAuditRepository) CreateControlPlaneAudit(_ context.Context, audit *domain.ControlPlaneAudit) error {
	if f.beginErr != nil {
		return f.beginErr
	}
	audit.ID = uint64(len(f.created) + 1)
	f.created = append(f.created, audit)
	return nil
}

func (f *fakeControlPlaneAuditRepository) CompleteControlPlaneAudit(_ context.Context, id uint64, statusCode int, outcome string, _ time.Time) error {
	f.completedID = id
	f.statusCode = statusCode
	f.outcome = outcome
	return f.completeErr
}

func (f *fakeControlPlaneAuditRepository) ListControlPlaneAudits(context.Context, int, int) ([]domain.ControlPlaneAudit, error) {
	return nil, nil
}

func (f *fakeControlPlaneAuditRepository) ListControlPlaneAuditsAfterSequence(context.Context, uint64, int) ([]domain.ControlPlaneAudit, error) {
	return nil, nil
}

func (f *fakeControlPlaneAuditRepository) GetControlPlaneAuditCheckpoint(context.Context) (domain.ControlPlaneAuditChainState, error) {
	return domain.ControlPlaneAuditChainState{}, nil
}

func (f *fakeControlPlaneAuditRepository) SealUnchainedControlPlaneAudits(context.Context, int) (int, error) {
	return 0, nil
}

func (f *fakeControlPlaneAuditRepository) VerifyControlPlaneAuditChain(context.Context) (domain.ControlPlaneAuditIntegrityReport, error) {
	return domain.ControlPlaneAuditIntegrityReport{Valid: true}, nil
}

func (f *fakeControlPlaneAuditRepository) PurgeControlPlaneAuditsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func (f *fakeControlPlaneAuditRepository) ListControlPlaneAuditDeliveries(context.Context, string, int) ([]domain.ControlPlaneAuditDelivery, error) {
	return nil, nil
}

func (f *fakeControlPlaneAuditRepository) ListControlPlaneAuditDeliveryReceipts(context.Context, uint64, int) ([]domain.ControlPlaneAuditDeliveryReceipt, error) {
	return nil, nil
}

func (f *fakeControlPlaneAuditRepository) ListControlPlaneAuditDeliveryDLQ(context.Context, int) ([]domain.ControlPlaneAuditDeliveryDLQ, error) {
	return nil, nil
}

func (f *fakeControlPlaneAuditRepository) ReplayControlPlaneAuditDeliveryDLQ(context.Context, uint64, time.Time) error {
	return nil
}

func (f *fakeControlPlaneAuditRepository) ControlPlaneAuditDeliveryStats(context.Context) (domain.ControlPlaneAuditDeliveryStats, error) {
	return domain.ControlPlaneAuditDeliveryStats{}, nil
}

func TestControlPlaneAuditPersistsAuthenticatedIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeControlPlaneAuditRepository{}
	audits := service.NewControlPlaneAuditService(repo)
	r := gin.New()
	r.Use(requestID())
	group := r.Group("/api/v1")
	group.Use(
		controlPlaneBearerAuth(ControlPlaneAuthConfig{
			Enabled:     true,
			BearerToken: "active-secret",
			OperatorID:  "platform-admin",
		}),
		controlPlaneAudit(audits, ControlPlaneAuditConfig{Enabled: true}),
	)
	group.PUT("/ai/routing-policy", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	req := httptest.NewRequest(http.MethodPut, "/api/v1/ai/routing-policy", nil)
	req.Header.Set("Authorization", "Bearer active-secret")
	req.Header.Set("X-Request-ID", "phase23-request")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if len(repo.created) != 1 {
		t.Fatalf("created audits = %d, want 1", len(repo.created))
	}
	audit := repo.created[0]
	if audit.OperatorID != "platform-admin" || audit.CredentialSlot != "active" || audit.Method != http.MethodPut || audit.Route != "/api/v1/ai/routing-policy" || audit.Target != "/api/v1/ai/routing-policy" || audit.RequestID != "phase23-request" || audit.Outcome != "started" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	if repo.completedID != audit.ID || repo.statusCode != http.StatusNoContent || repo.outcome != "success" {
		t.Fatalf("unexpected completion: id=%d status=%d outcome=%q", repo.completedID, repo.statusCode, repo.outcome)
	}
}

func TestControlPlaneAuditFailsClosedBeforeMutation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &fakeControlPlaneAuditRepository{beginErr: errors.New("db unavailable")}
	audits := service.NewControlPlaneAuditService(repo)
	r := gin.New()
	r.Use(requestID())
	called := false
	group := r.Group("/admin")
	group.Use(
		controlPlaneBearerAuth(ControlPlaneAuthConfig{Enabled: true, BearerToken: "secret", OperatorID: "admin"}),
		controlPlaneAudit(audits, ControlPlaneAuditConfig{Enabled: true}),
	)
	group.POST("/mutation", func(c *gin.Context) {
		called = true
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/mutation", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
	if called {
		t.Fatal("mutation handler ran despite failed audit begin")
	}
}

func TestControlPlaneAuditOutcome(t *testing.T) {
	for status, want := range map[int]string{200: "success", 409: "client_error", 503: "server_error"} {
		if got := controlPlaneAuditOutcome(status); got != want {
			t.Fatalf("controlPlaneAuditOutcome(%d) = %q, want %q", status, got, want)
		}
	}
}
