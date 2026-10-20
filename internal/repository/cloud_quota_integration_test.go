package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/testsupport"
)

func TestCloudQuotaIntegration(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("TASKFLOW_MYSQL_INTEGRATION_DSN is not set")
	}
	if !testsupport.DisposableMySQLDSN(dsn) {
		t.Fatal("unsafe integration DSN: database must be an explicit *_ci or *_test schema")
	}

	db, err := NewMySQL(dsn, 20, 10, true)
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&domain.CloudResource{}, &domain.CloudOrder{}, &domain.CloudTenantQuota{}, &domain.CloudTenantQuotaBucket{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	repo := NewCloudResourceRepository(db)
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenant := "quota-main-" + suffix

	quota, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 2000, MemoryMiB: 2048, Replicas: 4})
	if err != nil {
		t.Fatalf("set quota: %v", err)
	}
	assertQuota(t, quota, 2000, 2048, 4, 0, 0, 0)

	resource := testCloudResource(tenant, "app-main-"+suffix, `{"image":"nginx:1.27","replicas":2,"cpu":"500m","memory":"256Mi"}`)
	order := testCloudOrder(tenant, domain.CloudOrderCreate, "quota-create-"+suffix)
	if err := repo.CreateResourceWithOrder(ctx, resource, order, cloudcontrol.QuotaDemand{CPUMilli: 1000, MemoryMiB: 512, Replicas: 2}); err != nil {
		t.Fatalf("create with quota: %v", err)
	}
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 1000, 512, 2)

	over := testCloudResource(tenant, "app-over-"+suffix, `{"image":"nginx:1.27","replicas":2,"cpu":"750m","memory":"256Mi"}`)
	overOrder := testCloudOrder(tenant, domain.CloudOrderCreate, "quota-over-"+suffix)
	err = repo.CreateResourceWithOrder(ctx, over, overOrder, cloudcontrol.QuotaDemand{CPUMilli: 1500, MemoryMiB: 512, Replicas: 2})
	if !errors.Is(err, ErrCloudQuotaExceeded) {
		t.Fatalf("expected quota exceeded, got %v", err)
	}
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 1000, 512, 2)

	upOrder := testCloudOrder(tenant, domain.CloudOrderResize, "quota-up-"+suffix)
	resource, err = repo.ResizeResourceWithOrder(
		ctx,
		resource.ID,
		`{"image":"nginx:1.27","replicas":3,"cpu":"600m","memory":"341Mi"}`,
		cloudcontrol.QuotaDemand{CPUMilli: 1800, MemoryMiB: 1023, Replicas: 3},
		upOrder,
	)
	if err != nil {
		t.Fatalf("upscale reserve: %v", err)
	}
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 1800, 1023, 3)

	downOrder := testCloudOrder(tenant, domain.CloudOrderResize, "quota-down-"+suffix)
	resource, err = repo.ResizeResourceWithOrder(
		ctx,
		resource.ID,
		`{"image":"nginx:1.27","replicas":1,"cpu":"500m","memory":"256Mi"}`,
		cloudcontrol.QuotaDemand{CPUMilli: 500, MemoryMiB: 256, Replicas: 1},
		downOrder,
	)
	if err != nil {
		t.Fatalf("downscale desired state: %v", err)
	}
	// Downscale does not release capacity before the provider converges.
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 1800, 1023, 3)
	if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 1700, MemoryMiB: 2048, Replicas: 4}); !errors.Is(err, ErrCloudQuotaBelowUsage) {
		t.Fatalf("expected lowering quota below charged usage to fail, got %v", err)
	}

	completed, err := repo.CompleteReconcile(
		ctx,
		resource.ID,
		resource.Generation,
		domain.CloudResourceActive,
		`{"available_replicas":1}`,
		"provider-uid-main",
		cloudcontrol.QuotaDemand{CPUMilli: 500, MemoryMiB: 256, Replicas: 1},
	)
	if err != nil || !completed {
		t.Fatalf("complete downscale: completed=%v err=%v", completed, err)
	}
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 500, 256, 1)

	deleteOrder := testCloudOrder(tenant, domain.CloudOrderDelete, "quota-delete-"+suffix)
	resource, err = repo.DeleteResourceWithOrder(ctx, resource.ID, deleteOrder)
	if err != nil {
		t.Fatalf("delete desired state: %v", err)
	}
	// Delete also holds quota until the provider confirms the resource is gone.
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 500, 256, 1)
	completed, err = repo.CompleteReconcile(
		ctx,
		resource.ID,
		resource.Generation,
		domain.CloudResourceDeleted,
		`{}`,
		"provider-uid-main",
		cloudcontrol.QuotaDemand{},
	)
	if err != nil || !completed {
		t.Fatalf("complete delete: completed=%v err=%v", completed, err)
	}
	quota = mustQuota(t, repo, ctx, tenant)
	assertQuota(t, quota, 2000, 2048, 4, 0, 0, 0)

	// Prove the SELECT ... FOR UPDATE quota row serializes concurrent admission:
	// a one-replica tenant must admit exactly one of two simultaneous creates.
	concurrentTenant := "quota-race-" + suffix
	if _, err := repo.SetTenantQuota(ctx, concurrentTenant, cloudcontrol.QuotaDemand{CPUMilli: 1000, MemoryMiB: 1024, Replicas: 1}); err != nil {
		t.Fatalf("set concurrent quota: %v", err)
	}
	var admitted atomic.Int32
	var rejected atomic.Int32
	var unexpected atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			candidate := testCloudResource(concurrentTenant, fmt.Sprintf("race-%d-%s", i, suffix), `{"image":"nginx:1.27","replicas":1,"cpu":"500m","memory":"128Mi"}`)
			candidateOrder := testCloudOrder(concurrentTenant, domain.CloudOrderCreate, fmt.Sprintf("quota-race-%d-%s", i, suffix))
			err := repo.CreateResourceWithOrder(ctx, candidate, candidateOrder, cloudcontrol.QuotaDemand{CPUMilli: 500, MemoryMiB: 128, Replicas: 1})
			switch {
			case err == nil:
				admitted.Add(1)
			case errors.Is(err, ErrCloudQuotaExceeded):
				rejected.Add(1)
			default:
				t.Logf("unexpected concurrent create error: %v", err)
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if admitted.Load() != 1 || rejected.Load() != 1 || unexpected.Load() != 0 {
		t.Fatalf("concurrent admission admitted=%d rejected=%d unexpected=%d", admitted.Load(), rejected.Load(), unexpected.Load())
	}
	quota = mustQuota(t, repo, ctx, concurrentTenant)
	assertQuota(t, quota, 1000, 1024, 1, 500, 128, 1)

	t.Run("atomic versioned admission never oversubscribes", func(t *testing.T) {
		if err := repo.SetQuotaAdmissionMode(CloudQuotaAdmissionAtomicVersioned); err != nil {
			t.Fatal(err)
		}
		tenant := "quota-atomic-" + suffix
		if _, err := repo.SetTenantQuota(ctx, tenant, cloudcontrol.QuotaDemand{CPUMilli: 80, MemoryMiB: 8, Replicas: 8}); err != nil {
			t.Fatal(err)
		}
		var admitted atomic.Int32
		var rejected atomic.Int32
		var unexpected atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				resource := testCloudResource(tenant, fmt.Sprintf("atomic-%02d-%s", i, suffix), `{}`)
				order := testCloudOrder(tenant, domain.CloudOrderCreate, fmt.Sprintf("quota-atomic-%02d-%s", i, suffix))
				err := repo.CreateResourceWithOrder(ctx, resource, order, cloudcontrol.QuotaDemand{CPUMilli: 10, MemoryMiB: 1, Replicas: 1})
				switch {
				case err == nil:
					admitted.Add(1)
				case errors.Is(err, ErrCloudQuotaExceeded):
					rejected.Add(1)
				default:
					t.Logf("unexpected atomic admission error: %v", err)
					unexpected.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if admitted.Load() != 8 || rejected.Load() != 24 || unexpected.Load() != 0 {
			t.Fatalf("atomic admission admitted=%d rejected=%d unexpected=%d", admitted.Load(), rejected.Load(), unexpected.Load())
		}
		quota := mustQuota(t, repo, ctx, tenant)
		assertQuota(t, quota, 80, 8, 8, 80, 8, 8)
		if quota.Version < 9 {
			t.Fatalf("quota version=%d want at least 9 after eight successful reservations", quota.Version)
		}
	})

	t.Run("bucketed admission spreads hot tenant without oversubscription", func(t *testing.T) {
		if err := repo.SetQuotaBucketCount(8); err != nil {
			t.Fatal(err)
		}
		if err := repo.SetQuotaAdmissionMode(CloudQuotaAdmissionBucketed); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = repo.SetQuotaAdmissionMode(CloudQuotaAdmissionLocked)
			_ = repo.SetQuotaBucketCount(16)
		}()

		tenant := "quota-bucketed-" + suffix
		limits := cloudcontrol.QuotaDemand{CPUMilli: 800, MemoryMiB: 80, Replicas: 80}
		if _, err := repo.SetTenantQuota(ctx, tenant, limits); err != nil {
			t.Fatal(err)
		}

		var admitted atomic.Int32
		var rejected atomic.Int32
		var unexpected atomic.Int32
		var resourcesMu sync.Mutex
		resources := make([]*domain.CloudResource, 0, 80)
		candidates := make([]*domain.CloudResource, 0, 160)
		perBucket := make(map[int]int, 8)
		for i := 0; len(candidates) < 160; i++ {
			resource := testCloudResource(tenant, fmt.Sprintf("bucket-%04d-%s", i, suffix), `{}`)
			bucketID := quotaBucketStart(resource, 8)
			if perBucket[bucketID] >= 20 {
				continue
			}
			perBucket[bucketID]++
			candidates = append(candidates, resource)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range candidates {
			i := i
			resource := candidates[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				order := testCloudOrder(tenant, domain.CloudOrderCreate, fmt.Sprintf("quota-bucket-%03d-%s", i, suffix))
				err := repo.CreateResourceWithOrder(ctx, resource, order, cloudcontrol.QuotaDemand{CPUMilli: 10, MemoryMiB: 1, Replicas: 1})
				switch {
				case err == nil:
					if resource.QuotaBucketID == 0 {
						unexpected.Add(1)
						return
					}
					resourcesMu.Lock()
					resources = append(resources, resource)
					resourcesMu.Unlock()
					admitted.Add(1)
				case errors.Is(err, ErrCloudQuotaExceeded):
					rejected.Add(1)
				default:
					t.Logf("unexpected bucketed admission error: %v", err)
					unexpected.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if admitted.Load() != 80 || rejected.Load() != 80 || unexpected.Load() != 0 {
			t.Fatalf("bucketed admission admitted=%d rejected=%d unexpected=%d", admitted.Load(), rejected.Load(), unexpected.Load())
		}

		quota := mustQuota(t, repo, ctx, tenant)
		assertQuota(t, quota, 800, 80, 80, 800, 80, 80)
		var activeBuckets int64
		if err := db.Model(&domain.CloudTenantQuotaBucket{}).
			Where("tenant_id = ? AND replica_used > 0", tenant).
			Count(&activeBuckets).Error; err != nil {
			t.Fatal(err)
		}
		if activeBuckets != 8 {
			t.Fatalf("active bucket count=%d want=8", activeBuckets)
		}

		if err := repo.SetQuotaAdmissionMode(CloudQuotaAdmissionLocked); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.SetTenantQuota(ctx, tenant, limits); !errors.Is(err, ErrCloudQuotaModeMismatch) {
			t.Fatalf("expected mode migration with live bucket usage to fail, got %v", err)
		}
		if err := repo.SetQuotaAdmissionMode(CloudQuotaAdmissionBucketed); err != nil {
			t.Fatal(err)
		}

		resourcesMu.Lock()
		victim := resources[0]
		resourcesMu.Unlock()
		var before domain.CloudTenantQuotaBucket
		if err := db.First(&before, "tenant_id = ? AND bucket_id = ?", tenant, victim.QuotaBucketID).Error; err != nil {
			t.Fatal(err)
		}
		deleteOrder := testCloudOrder(tenant, domain.CloudOrderDelete, "quota-bucket-delete-"+suffix)
		victim, err = repo.DeleteResourceWithOrder(ctx, victim.ID, deleteOrder)
		if err != nil {
			t.Fatal(err)
		}
		completed, err := repo.CompleteReconcile(ctx, victim.ID, victim.Generation, domain.CloudResourceDeleted, `{}`, "", cloudcontrol.QuotaDemand{})
		if err != nil || !completed {
			t.Fatalf("bucketed delete completion completed=%v err=%v", completed, err)
		}
		var after domain.CloudTenantQuotaBucket
		if err := db.First(&after, "tenant_id = ? AND bucket_id = ?", tenant, victim.QuotaBucketID).Error; err != nil {
			t.Fatal(err)
		}
		if before.CPUMilliUsed-after.CPUMilliUsed != 10 || before.MemoryMiBUsed-after.MemoryMiBUsed != 1 || before.ReplicaUsed-after.ReplicaUsed != 1 {
			t.Fatalf("release did not return to same bucket before=%+v after=%+v", before, after)
		}
		quota = mustQuota(t, repo, ctx, tenant)
		assertQuota(t, quota, 800, 80, 80, 790, 79, 79)
	})

	t.Run("bucketed admission probes around hash skew without oversubscription", func(t *testing.T) {
		if err := repo.SetQuotaBucketCount(4); err != nil {
			t.Fatal(err)
		}
		if err := repo.SetQuotaAdmissionMode(CloudQuotaAdmissionBucketed); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = repo.SetQuotaAdmissionMode(CloudQuotaAdmissionLocked)
			_ = repo.SetQuotaBucketCount(16)
		}()

		tenant := "quota-bucket-skew-" + suffix
		limits := cloudcontrol.QuotaDemand{CPUMilli: 80, MemoryMiB: 8, Replicas: 8}
		if _, err := repo.SetTenantQuota(ctx, tenant, limits); err != nil {
			t.Fatal(err)
		}

		resources := make([]*domain.CloudResource, 0, 9)
		for i := 0; len(resources) < 9; i++ {
			resource := testCloudResource(tenant, fmt.Sprintf("skew-%05d-%s", i, suffix), `{}`)
			if quotaBucketStart(resource, 4) == 1 {
				resources = append(resources, resource)
			}
		}
		for i := 0; i < 8; i++ {
			order := testCloudOrder(tenant, domain.CloudOrderCreate, fmt.Sprintf("quota-bucket-skew-%02d-%s", i, suffix))
			if err := repo.CreateResourceWithOrder(ctx, resources[i], order, cloudcontrol.QuotaDemand{CPUMilli: 10, MemoryMiB: 1, Replicas: 1}); err != nil {
				t.Fatalf("admit skewed resource %d: %v", i, err)
			}
		}
		ninthOrder := testCloudOrder(tenant, domain.CloudOrderCreate, "quota-bucket-skew-overflow-"+suffix)
		if err := repo.CreateResourceWithOrder(ctx, resources[8], ninthOrder, cloudcontrol.QuotaDemand{CPUMilli: 10, MemoryMiB: 1, Replicas: 1}); !errors.Is(err, ErrCloudQuotaExceeded) {
			t.Fatalf("ninth skewed resource err=%v want quota exceeded", err)
		}

		quota := mustQuota(t, repo, ctx, tenant)
		assertQuota(t, quota, 80, 8, 8, 80, 8, 8)
		var activeBuckets int64
		if err := db.Model(&domain.CloudTenantQuotaBucket{}).
			Where("tenant_id = ? AND replica_used > 0", tenant).
			Count(&activeBuckets).Error; err != nil {
			t.Fatal(err)
		}
		if activeBuckets != 4 {
			t.Fatalf("active bucket count=%d want=4", activeBuckets)
		}
	})
}

func testCloudResource(tenant, name, spec string) *domain.CloudResource {
	return &domain.CloudResource{
		TenantID:     tenant,
		Name:         name,
		Kind:         "deployment",
		Provider:     "kubernetes",
		DesiredSpec:  spec,
		DesiredState: domain.CloudResourceDesiredActive,
		ActualState:  domain.CloudResourcePending,
		Generation:   1,
	}
}

func testCloudOrder(tenant string, orderType domain.CloudOrderType, key string) *domain.CloudOrder {
	return &domain.CloudOrder{
		TenantID:       tenant,
		Type:           orderType,
		Status:         domain.CloudOrderPending,
		IdempotencyKey: key,
	}
}

func mustQuota(t *testing.T, repo *CloudResourceRepository, ctx context.Context, tenant string) *domain.CloudTenantQuota {
	t.Helper()
	quota, err := repo.GetTenantQuota(ctx, tenant)
	if err != nil {
		t.Fatalf("get quota for %s: %v", tenant, err)
	}
	return quota
}

func assertQuota(t *testing.T, quota *domain.CloudTenantQuota, cpuLimit, memoryLimit, replicaLimit, cpuUsed, memoryUsed, replicaUsed int64) {
	t.Helper()
	if quota.CPUMilliLimit != cpuLimit || quota.MemoryMiBLimit != memoryLimit || quota.ReplicaLimit != replicaLimit ||
		quota.CPUMilliUsed != cpuUsed || quota.MemoryMiBUsed != memoryUsed || quota.ReplicaUsed != replicaUsed {
		t.Fatalf("unexpected quota: %+v", quota)
	}
}
