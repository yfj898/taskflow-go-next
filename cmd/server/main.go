package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	archivepkg "github.com/yfj898/taskflow-go/internal/archive"
	"github.com/yfj898/taskflow-go/internal/cloudcontrol"
	"github.com/yfj898/taskflow-go/internal/config"
	"github.com/yfj898/taskflow-go/internal/domain"
	"github.com/yfj898/taskflow-go/internal/executor"
	"github.com/yfj898/taskflow-go/internal/handler"
	appmetrics "github.com/yfj898/taskflow-go/internal/metrics"
	"github.com/yfj898/taskflow-go/internal/monitor"
	"github.com/yfj898/taskflow-go/internal/outbox"
	"github.com/yfj898/taskflow-go/internal/queue"
	"github.com/yfj898/taskflow-go/internal/reconciler"
	"github.com/yfj898/taskflow-go/internal/repository"
	"github.com/yfj898/taskflow-go/internal/schemamigrate"
	"github.com/yfj898/taskflow-go/internal/service"
	"github.com/yfj898/taskflow-go/internal/siem"
	apptelemetry "github.com/yfj898/taskflow-go/internal/telemetry"
	"github.com/yfj898/taskflow-go/internal/verification"
	"github.com/yfj898/taskflow-go/internal/worker"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}
	verificationConfig, err := verification.Load(os.Getenv("TASKFLOW_VERIFICATION_CONFIG"))
	if err != nil {
		log.Fatalf("verification config: %v", err)
	}
	if verificationConfig != nil && (!cfg.ControlPlaneAuthEnabled || cfg.ExecutionBackend != "local" || runtime.GOOS != "linux") {
		log.Fatal("verification requires enabled authentication, Linux and EXECUTION_BACKEND=local")
	}
	var draining atomic.Bool
	appmetrics.ProcessDraining.Set(0)
	shutdownTracing, err := apptelemetry.Init(context.Background(), "taskflow-api", cfg.OTelLogSpans)
	if err != nil {
		log.Fatalf("init telemetry: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	db, err := repository.NewMySQL(cfg.MySQLDSN, cfg.MySQLMaxOpenConns, cfg.MySQLMaxIdleConns, cfg.MySQLSkipDefaultTx)
	if strings.TrimSpace(cfg.MySQLFailoverDSNs) != "" {
		var selected int
		db, selected, err = repository.NewMySQLWithFailover(
			cfg.MySQLDSN,
			cfg.MySQLFailoverDSNs,
			cfg.MySQLMaxOpenConns,
			cfg.MySQLMaxIdleConns,
			cfg.MySQLSkipDefaultTx,
			time.Duration(cfg.MySQLConnectTimeoutMS)*time.Millisecond,
		)
		if err == nil {
			log.Printf("mysql writer startup candidate selected=%d", selected)
		}
	}
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("unwrap mysql connection pool: %v", err)
	}
	appmetrics.RegisterDBPool(sqlDB)

	taskRepo := repository.NewTaskRepository(db)
	taskRepo.ConfigureControlPlaneSIEMDelivery(cfg.ControlSIEMEnabled)
	taskRepo.ConfigureManagedWorkflowSchema(cfg.CloudWorkflowSchemaManaged)
	if err := taskRepo.Migrate(); err != nil {
		log.Fatalf("migrate tasks: %v", err)
	}
	if verificationConfig != nil {
		if err := taskRepo.VerifyVerificationSchema(); err != nil {
			log.Fatalf("verification schema: %v", err)
		}
	}
	if cfg.CloudTenantAuthzEnabled {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		status, err := schemamigrate.Inspect(ctx, sqlDB)
		cancel()
		if err != nil || !status.CompositeReady || len(status.LegacyUnique) != 0 {
			log.Fatalf("CLOUD_TENANT_AUTHZ_ENABLED requires tenant-scoped cloud_orders uniqueness; run explicit expand/contract migration after stopping legacy writers: schema=%+v error=%v", status, err)
		}
	}

	rdb := queue.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("connect redis: %v", err)
	}

	var taskQueue queue.TaskQueue
	switch strings.ToLower(strings.TrimSpace(cfg.QueueBackend)) {
	case "redis", "redis-streams", "redis_streams":
		taskQueue = queue.NewRedisTaskQueue(rdb, cfg.RedisQueueKey)
	case "kafka":
		kafkaQueue := queue.NewKafkaTaskQueue(strings.Split(cfg.KafkaBrokers, ","), cfg.KafkaTopic, cfg.KafkaGroup)
		defer kafkaQueue.Close()
		taskQueue = kafkaQueue
	default:
		log.Fatalf("unsupported QUEUE_BACKEND %q", cfg.QueueBackend)
	}
	taskService := service.NewTaskService(taskRepo, taskQueue)
	cloudRepo := repository.NewCloudResourceRepository(db)
	quotaAdmissionMode := strings.TrimSpace(os.Getenv("CLOUD_QUOTA_ADMISSION_MODE"))
	if err := cloudRepo.SetQuotaAdmissionMode(quotaAdmissionMode); err != nil {
		log.Fatalf("configure cloud quota admission: %v", err)
	}
	quotaBucketCount := 16
	if raw := strings.TrimSpace(os.Getenv("CLOUD_QUOTA_BUCKETS")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil {
			log.Fatalf("invalid CLOUD_QUOTA_BUCKETS %q", raw)
		}
		quotaBucketCount = parsed
	}
	if err := cloudRepo.SetQuotaBucketCount(quotaBucketCount); err != nil {
		log.Fatalf("configure cloud quota buckets: %v", err)
	}
	workflowDBMode := strings.TrimSpace(os.Getenv("CLOUD_SAGA_DB_MODE"))
	if err := cloudRepo.SetWorkflowDBMode(workflowDBMode); err != nil {
		log.Fatalf("configure cloud Saga DB mode: %v", err)
	}
	if quotaAdmissionMode == "" {
		quotaAdmissionMode = repository.CloudQuotaAdmissionLocked
	}
	if workflowDBMode == "" {
		workflowDBMode = repository.CloudWorkflowDBBaseline
	}
	log.Printf("cloud-control persistence modes quota_admission=%s quota_buckets=%d saga_db=%s", quotaAdmissionMode, quotaBucketCount, workflowDBMode)
	cloudProvider, cloudControlEnabled, err := cloudcontrol.NewKubernetesProviderFromEnvironment()
	if err != nil {
		log.Fatalf("init cloud-control kubernetes provider: %v", err)
	}
	var cloudResourceProvider cloudcontrol.ResourceProvider
	var cloudSagaProvider cloudcontrol.SagaResourceProvider
	if cloudProvider != nil {
		cloudResourceProvider = cloudProvider
		cloudSagaProvider, err = cloudcontrol.WrapSagaProviderWithFaultInjectionFromEnvironment(cloudProvider)
		if err != nil {
			log.Fatalf("init cloud Saga fault injection: %v", err)
		}
	}
	cloudResourceService := service.NewCloudResourceService(cloudRepo, cloudResourceProvider)
	cloudWorkflowService := service.NewCloudWorkflowService(cloudRepo, cloudSagaProvider)
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_LEASE_MS")); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms <= 0 {
			log.Fatalf("invalid CLOUD_SAGA_LEASE_MS %q", raw)
		}
		cloudWorkflowService.SetLeaseDuration(time.Duration(ms) * time.Millisecond)
	}
	sagaWorkers := 1
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_WORKER_CONCURRENCY")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 || parsed > 64 {
			log.Fatalf("invalid CLOUD_SAGA_WORKER_CONCURRENCY %q", raw)
		}
		sagaWorkers = parsed
	}
	cloudWorkflowService.SetWorkerConcurrency(sagaWorkers)
	observeBackoffMS := 0
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_OBSERVE_BACKOFF_MS")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 0 || parsed > 60000 {
			log.Fatalf("invalid CLOUD_SAGA_OBSERVE_BACKOFF_MS %q", raw)
		}
		observeBackoffMS = parsed
	}
	observeBackoffMaxMS := 2000
	if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_OBSERVE_BACKOFF_MAX_MS")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 || parsed > 300000 {
			log.Fatalf("invalid CLOUD_SAGA_OBSERVE_BACKOFF_MAX_MS %q", raw)
		}
		observeBackoffMaxMS = parsed
	}
	cloudWorkflowService.SetObserveBackoff(
		time.Duration(observeBackoffMS)*time.Millisecond,
		time.Duration(observeBackoffMaxMS)*time.Millisecond,
	)
	deadLetterService := service.NewDeadLetterService(
		taskRepo,
		taskQueue,
		time.Duration(cfg.QueueDLQReplayLeaseMS)*time.Millisecond,
		time.Duration(cfg.EnqueueLeaseMS)*time.Millisecond,
	)
	if cfg.DLQArchiveEnabled {
		var archiveSink archivepkg.Sink
		switch strings.ToLower(strings.TrimSpace(cfg.DLQArchiveSink)) {
		case "file", "filesystem":
			archiveSink, err = archivepkg.NewFileSink(cfg.DLQArchiveDir)
		case "http", "object-http", "object_http":
			archiveSink, err = archivepkg.NewHTTPObjectSink(
				cfg.DLQArchiveHTTPBaseURL,
				cfg.DLQArchiveHTTPToken,
				time.Duration(cfg.DLQArchiveHTTPTimeoutMS)*time.Millisecond,
			)
		case "s3", "minio", "s3-compatible", "s3_compatible":
			archiveSink, err = archivepkg.NewS3Sink(
				cfg.DLQArchiveS3Endpoint,
				cfg.DLQArchiveS3AccessKey,
				cfg.DLQArchiveS3SecretKey,
				cfg.DLQArchiveS3Bucket,
				cfg.DLQArchiveS3Region,
				cfg.DLQArchiveS3Secure,
			)
		default:
			log.Fatalf("unsupported QUEUE_DLQ_ARCHIVE_SINK %q", cfg.DLQArchiveSink)
		}
		if err != nil {
			log.Fatalf("init DLQ archive sink: %v", err)
		}
		var archiveReplicas []archivepkg.Sink
		if cfg.DLQReplicaEnabled {
			var replica archivepkg.Sink
			switch strings.ToLower(strings.TrimSpace(cfg.DLQReplicaSink)) {
			case "file", "filesystem":
				replica, err = archivepkg.NewFileSink(cfg.DLQReplicaDir)
			case "http", "object-http", "object_http":
				replica, err = archivepkg.NewHTTPObjectSink(
					cfg.DLQReplicaHTTPBaseURL,
					cfg.DLQReplicaHTTPToken,
					time.Duration(cfg.DLQReplicaHTTPTimeoutMS)*time.Millisecond,
				)
			case "s3", "minio", "s3-compatible", "s3_compatible":
				replica, err = archivepkg.NewS3Sink(
					cfg.DLQReplicaS3Endpoint,
					cfg.DLQReplicaS3AccessKey,
					cfg.DLQReplicaS3SecretKey,
					cfg.DLQReplicaS3Bucket,
					cfg.DLQReplicaS3Region,
					cfg.DLQReplicaS3Secure,
				)
			default:
				log.Fatalf("unsupported QUEUE_DLQ_ARCHIVE_REPLICA_SINK %q", cfg.DLQReplicaSink)
			}
			if err != nil {
				log.Fatalf("init DLQ archive replica sink: %v", err)
			}
			archiveReplicas = append(archiveReplicas, archivepkg.WithName("replica:"+replica.Name(), replica))
		}
		if cfg.DLQArchiveMinCopies > 1+len(archiveReplicas) {
			log.Fatalf("QUEUE_DLQ_ARCHIVE_MIN_COPIES=%d exceeds configured archive copies=%d", cfg.DLQArchiveMinCopies, 1+len(archiveReplicas))
		}
		if cfg.DLQObjLifecycleEnabled && cfg.DLQOrphanGraceMS > 0 && cfg.DLQOrphanGraceMS < cfg.DLQArchiveLeaseMS {
			log.Fatalf("QUEUE_DLQ_ARCHIVE_ORPHAN_GRACE_MS=%d must be >= QUEUE_DLQ_ARCHIVE_LEASE_MS=%d", cfg.DLQOrphanGraceMS, cfg.DLQArchiveLeaseMS)
		}
		deadLetterService.ConfigureArchiveReplication(
			archiveSink,
			archiveReplicas,
			time.Duration(cfg.DLQArchiveLeaseMS)*time.Millisecond,
			cfg.DLQArchiveMinCopies,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cloudControlEnabled {
		go cloudResourceService.RunController(ctx, 2*time.Second, 50)
		log.Printf("cloud-control reconciler enabled provider=kubernetes")
		sagaControllerEnabled := true
		if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_CONTROLLER_ENABLED")); raw != "" {
			if parsed, parseErr := strconv.ParseBool(raw); parseErr == nil {
				sagaControllerEnabled = parsed
			} else {
				log.Fatalf("invalid CLOUD_SAGA_CONTROLLER_ENABLED %q: %v", raw, parseErr)
			}
		}
		if sagaControllerEnabled {
			interval := time.Second
			if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_RECONCILE_INTERVAL_MS")); raw != "" {
				ms, parseErr := strconv.Atoi(raw)
				if parseErr != nil || ms <= 0 {
					log.Fatalf("invalid CLOUD_SAGA_RECONCILE_INTERVAL_MS %q", raw)
				}
				interval = time.Duration(ms) * time.Millisecond
			}
			batch := 50
			if raw := strings.TrimSpace(os.Getenv("CLOUD_SAGA_RECONCILE_BATCH")); raw != "" {
				parsed, parseErr := strconv.Atoi(raw)
				if parseErr != nil || parsed <= 0 || parsed > 200 {
					log.Fatalf("invalid CLOUD_SAGA_RECONCILE_BATCH %q", raw)
				}
				batch = parsed
			}
			go cloudWorkflowService.RunController(ctx, interval, batch)
			log.Printf("cloud-saga reconciler enabled provider=kubernetes interval=%s batch=%d", interval, batch)
		} else {
			log.Printf("cloud-saga reconciler disabled; manual reconcile API remains available")
		}
	}
	reconcileCtx, reconcileCancel := context.WithCancel(ctx)
	defer reconcileCancel()
	controlPlaneAudits := service.NewControlPlaneAuditService(taskRepo)
	var siemReceiverControl handler.SIEMReceiverControl
	if sealed, err := controlPlaneAudits.SealLegacy(ctx, 5000); err != nil {
		log.Fatalf("seal legacy control-plane audits: %v", err)
	} else if sealed > 0 {
		log.Printf("sealed %d legacy control-plane audit rows into the integrity chain", sealed)
	}
	if cfg.ControlSIEMEnabled {
		report, err := controlPlaneAudits.Verify(ctx)
		if err != nil {
			log.Fatalf("verify control-plane audit chain before SIEM delivery: %v", err)
		}
		if !report.Valid {
			log.Fatalf("control-plane audit chain invalid before SIEM delivery: %s", report.Failure)
		}
		for {
			backfilled, err := taskRepo.BackfillControlPlaneAuditDeliveryReceipts(ctx, 5000)
			if err != nil {
				log.Fatalf("backfill control-plane SIEM delivery receipts: %v", err)
			}
			if backfilled < 5000 {
				break
			}
		}
		for {
			backfilled, err := taskRepo.BackfillControlPlaneAuditDeliveries(ctx, 5000)
			if err != nil {
				log.Fatalf("backfill control-plane SIEM delivery outbox: %v", err)
			}
			if backfilled < 5000 {
				break
			}
		}
		var signer siem.CheckpointSigner
		switch strings.ToLower(strings.TrimSpace(cfg.SIEMSignProvider)) {
		case "hmac", "hmac-sha256":
			signer, err = siem.NewRotatingHMACSHA256Signer(
				cfg.ControlSIEMSignKeyID,
				cfg.ControlSIEMSignKey,
				cfg.SIEMPrevSignKeyID,
				cfg.SIEMPrevSignKey,
			)
		case "remote", "kms", "vault":
			signer, err = siem.NewRemoteCheckpointSigner(siem.RemoteSignerConfig{
				Endpoint:    cfg.SIEMRemoteEndpoint,
				BearerToken: cfg.SIEMRemoteToken,
				KeyID:       cfg.ControlSIEMSignKeyID,
				Algorithm:   cfg.SIEMRemoteAlgo,
				Timeout:     time.Duration(cfg.SIEMRemoteTimeoutMS) * time.Millisecond,
			})
		case "aws-kms", "aws-kms-hmac":
			var credentialProvider siem.AWSCredentialProvider
			credentialSource := strings.ToLower(strings.TrimSpace(cfg.SIEMAWSCredentialsSource))
			if credentialSource == "" {
				credentialSource = "static"
			}
			staticProvider := func() siem.AWSCredentialProvider {
				if strings.TrimSpace(cfg.SIEMAWSKMSAccessKey) == "" || strings.TrimSpace(cfg.SIEMAWSKMSSecretKey) == "" {
					return nil
				}
				return siem.StaticAWSCredentialProvider{Credentials: siem.AWSCredentials{
					AccessKeyID:     cfg.SIEMAWSKMSAccessKey,
					SecretAccessKey: cfg.SIEMAWSKMSSecretKey,
					SessionToken:    cfg.SIEMAWSKMSSessionToken,
					Source:          "static",
				}}
			}
			webIdentityProvider := func() (siem.AWSCredentialProvider, error) {
				if strings.TrimSpace(cfg.SIEMAWSWebIdentityFile) == "" || strings.TrimSpace(cfg.SIEMAWSWebIdentityRoleARN) == "" {
					return nil, nil
				}
				return siem.NewAWSWebIdentityProvider(siem.AWSWebIdentityProviderConfig{
					TokenFile:       cfg.SIEMAWSWebIdentityFile,
					RoleARN:         cfg.SIEMAWSWebIdentityRoleARN,
					RoleSessionName: cfg.SIEMAWSWebIdentitySession,
					STSEndpoint:     cfg.SIEMAWSSTSEndpoint,
					Timeout:         time.Duration(cfg.SIEMAWSKMSTimeoutMS) * time.Millisecond,
				})
			}
			imdsProvider := func() (siem.AWSCredentialProvider, error) {
				return siem.NewAWSIMDSv2Provider(siem.AWSIMDSv2ProviderConfig{
					Endpoint: cfg.SIEMAWSIMDSEndpoint,
					Timeout:  time.Duration(cfg.SIEMAWSIMDSTimeoutMS) * time.Millisecond,
				})
			}
			switch credentialSource {
			case "static":
				credentialProvider = staticProvider()
			case "web-identity":
				credentialProvider, err = webIdentityProvider()
			case "imds", "imds-v2":
				credentialProvider, err = imdsProvider()
			case "auto":
				providers := make([]siem.AWSCredentialProvider, 0, 3)
				if provider := staticProvider(); provider != nil {
					providers = append(providers, provider)
				}
				if provider, providerErr := webIdentityProvider(); providerErr != nil {
					err = providerErr
				} else if provider != nil {
					providers = append(providers, provider)
				}
				if err == nil {
					if provider, providerErr := imdsProvider(); providerErr != nil {
						err = providerErr
					} else {
						providers = append(providers, provider)
					}
				}
				if err == nil {
					credentialProvider, err = siem.NewChainedAWSCredentialProvider(providers...)
				}
			}
			if err != nil {
				break
			}
			signer, err = siem.NewAWSKMSMACSigner(siem.AWSKMSMACSignerConfig{
				Endpoint:        cfg.SIEMAWSKMSEndpoint,
				Region:          cfg.SIEMAWSKMSRegion,
				AccessKeyID:     cfg.SIEMAWSKMSAccessKey,
				SecretAccessKey: cfg.SIEMAWSKMSSecretKey,
				SessionToken:    cfg.SIEMAWSKMSSessionToken,
				Credentials:     credentialProvider,
				KeyID:           cfg.SIEMAWSKMSKeyID,
				Timeout:         time.Duration(cfg.SIEMAWSKMSTimeoutMS) * time.Millisecond,
			})
		case "vault-transit":
			signer, err = siem.NewVaultTransitSigner(siem.VaultTransitSignerConfig{
				Endpoint:   cfg.SIEMVaultEndpoint,
				Token:      cfg.SIEMVaultToken,
				Namespace:  cfg.SIEMVaultNamespace,
				Mount:      cfg.SIEMVaultMount,
				KeyName:    cfg.SIEMVaultKeyName,
				KeyID:      cfg.ControlSIEMSignKeyID,
				KeyVersion: cfg.SIEMVaultKeyVersion,
				Timeout:    time.Duration(cfg.SIEMVaultTimeoutMS) * time.Millisecond,
			})
		default:
			log.Fatalf("unsupported control-plane SIEM signing provider %q", cfg.SIEMSignProvider)
		}
		if err != nil {
			log.Fatalf("init control-plane SIEM signer: %v", err)
		}
		var governance *siem.GovernanceClient
		if strings.TrimSpace(cfg.SIEMGovernanceEndpoint) != "" {
			governanceSigner, governanceErr := siem.NewHMACSHA256Signer(cfg.SIEMGovernanceSignKeyID, cfg.SIEMGovernanceSignKey)
			if governanceErr != nil {
				log.Fatalf("init control-plane SIEM governance signer: %v", governanceErr)
			}
			if cfg.SIEMGovernanceHAEnabled {
				secondaryToken := cfg.SIEMGovernanceSecondaryToken
				if strings.TrimSpace(secondaryToken) == "" {
					secondaryToken = cfg.SIEMGovernanceToken
				}
				primaryPreviousValidUntil := time.Time{}
				if cfg.SIEMGovernancePrimaryPrevReceiptValidUntilMS > 0 {
					primaryPreviousValidUntil = time.UnixMilli(cfg.SIEMGovernancePrimaryPrevReceiptValidUntilMS).UTC()
				}
				secondaryPreviousValidUntil := time.Time{}
				if cfg.SIEMGovernanceSecondaryPrevReceiptValidUntilMS > 0 {
					secondaryPreviousValidUntil = time.UnixMilli(cfg.SIEMGovernanceSecondaryPrevReceiptValidUntilMS).UTC()
				}
				governance, governanceErr = siem.NewGovernanceHAClient(
					[]siem.GovernanceReplicaConfig{
						{
							ID: cfg.SIEMGovernancePrimaryReplicaID, Endpoint: cfg.SIEMGovernanceEndpoint, Token: cfg.SIEMGovernanceToken,
							ReceiptKeyID: cfg.SIEMGovernancePrimaryReceiptKeyID, ReceiptHMACKey: cfg.SIEMGovernancePrimaryReceiptKey, ReceiptKeyGeneration: uint64(cfg.SIEMGovernancePrimaryReceiptKeyGeneration),
							PreviousReceiptKeyID: cfg.SIEMGovernancePrimaryPrevReceiptKeyID, PreviousReceiptHMACKey: cfg.SIEMGovernancePrimaryPrevReceiptKey, PreviousReceiptKeyGeneration: uint64(cfg.SIEMGovernancePrimaryPrevReceiptKeyGeneration), PreviousReceiptValidUntil: primaryPreviousValidUntil,
						},
						{
							ID: cfg.SIEMGovernanceSecondaryReplicaID, Endpoint: cfg.SIEMGovernanceSecondaryEndpoint, Token: secondaryToken,
							ReceiptKeyID: cfg.SIEMGovernanceSecondaryReceiptKeyID, ReceiptHMACKey: cfg.SIEMGovernanceSecondaryReceiptKey, ReceiptKeyGeneration: uint64(cfg.SIEMGovernanceSecondaryReceiptKeyGeneration),
							PreviousReceiptKeyID: cfg.SIEMGovernanceSecondaryPrevReceiptKeyID, PreviousReceiptHMACKey: cfg.SIEMGovernanceSecondaryPrevReceiptKey, PreviousReceiptKeyGeneration: uint64(cfg.SIEMGovernanceSecondaryPrevReceiptKeyGeneration), PreviousReceiptValidUntil: secondaryPreviousValidUntil,
						},
					},
					cfg.SIEMGovernanceReceiptQuorum,
					cfg.SIEMGovernanceMembershipEpoch,
					governanceSigner,
					time.Duration(cfg.SIEMGovernanceTimeoutMS)*time.Millisecond,
				)
			} else {
				governance, governanceErr = siem.NewGovernanceClient(
					cfg.SIEMGovernanceEndpoint,
					cfg.SIEMGovernanceToken,
					governanceSigner,
					time.Duration(cfg.SIEMGovernanceTimeoutMS)*time.Millisecond,
				)
			}
			if governanceErr != nil {
				log.Fatalf("init control-plane SIEM governance client: %v", governanceErr)
			}
		}
		var checkpointVerifier *siem.ReceiverCheckpointSignatureVerifier
		if strings.TrimSpace(cfg.SIEMCheckpointHMACKey) != "" || strings.TrimSpace(cfg.SIEMCheckpointHMACKeyID) != "" {
			checkpointVerifier, err = siem.NewReceiverCheckpointSignatureVerifier(cfg.SIEMCheckpointHMACKeyID, cfg.SIEMCheckpointHMACKey)
			if err != nil {
				log.Fatalf("init control-plane SIEM receiver checkpoint verifier: %v", err)
			}
		}
		var deliveryCoordinator siem.DeliveryCoordinator
		if cfg.SIEMCoordEnabled {
			deliveryCoordinator = siem.NewRedisFencingCoordinator(rdb, cfg.SIEMCoordKey)
		}
		var evidenceReplica siem.EvidenceReplica
		if cfg.SIEMDREnabled {
			primary, sinkErr := archivepkg.NewS3Sink(
				cfg.SIEMDRPrimaryEndpoint,
				cfg.SIEMDRPrimaryAccessKey,
				cfg.SIEMDRPrimarySecretKey,
				cfg.SIEMDRPrimaryBucket,
				cfg.SIEMDRPrimaryRegion,
				cfg.SIEMDRPrimarySecure,
			)
			if sinkErr != nil {
				log.Fatalf("init control-plane SIEM primary DR sink: %v", sinkErr)
			}
			sinks := []archivepkg.Sink{archivepkg.WithName("siem-dr-primary", primary)}
			if cfg.SIEMDRReplicaEnabled {
				replica, replicaErr := archivepkg.NewS3Sink(
					cfg.SIEMDRReplicaEndpoint,
					cfg.SIEMDRReplicaAccessKey,
					cfg.SIEMDRReplicaSecretKey,
					cfg.SIEMDRReplicaBucket,
					cfg.SIEMDRReplicaRegion,
					cfg.SIEMDRReplicaSecure,
				)
				if replicaErr != nil {
					log.Fatalf("init control-plane SIEM replica DR sink: %v", replicaErr)
				}
				sinks = append(sinks, archivepkg.WithName("siem-dr-replica", replica))
			}
			evidenceReplica, err = siem.NewMultiSinkEvidenceReplica(cfg.SIEMDRMinCopies, sinks...)
			if err != nil {
				log.Fatalf("init control-plane SIEM DR replication: %v", err)
			}
		}
		startOnSecondary := false
		if cfg.SIEMClusterGuardEnabled {
			clusterGuard, guardErr := siem.NewReceiverClusterGuard(taskRepo, siem.ReceiverClusterGuardConfig{
				PrimaryCheckpointEndpoint:   cfg.SIEMPrimaryCheckpoint,
				PrimaryReceiver:             cfg.SIEMPrimaryReceiverID,
				SecondaryCheckpointEndpoint: cfg.SIEMSecondaryCheckpoint,
				SecondaryReceiver:           cfg.SIEMSecondaryReceiverID,
				BearerToken:                 cfg.ControlSIEMToken,
				PageSize:                    cfg.SIEMReconcilePageSize,
				HTTPTimeout:                 time.Duration(cfg.SIEMReconcileTimeoutMS) * time.Millisecond,
				SignatureVerifier:           checkpointVerifier,
			})
			if guardErr != nil {
				log.Fatalf("init control-plane SIEM receiver cluster guard: %v", guardErr)
			}
			clusterState, guardErr := clusterGuard.Verify(ctx)
			if guardErr != nil {
				appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("cluster_guard_fenced").Inc()
				log.Fatalf("control-plane SIEM receiver cluster divergence: %v", guardErr)
			}
			startOnSecondary = clusterState.PreferSecondary
			appmetrics.ControlPlaneSIEMDeliveries.WithLabelValues("cluster_guard_passed").Inc()
			if startOnSecondary {
				log.Printf("control-plane SIEM receiver cluster starts on secondary: primary_hwm=%d secondary_hwm=%d", clusterState.PrimaryHighWatermark, clusterState.SecondaryHighWatermark)
			}
		}
		if cfg.SIEMReconcileEnabled {
			var evidenceVerifier siem.ReceiptEvidenceVerifier
			if cfg.SIEMDREnabled {
				var ok bool
				evidenceVerifier, ok = evidenceReplica.(siem.ReceiptEvidenceVerifier)
				if !ok {
					log.Fatalf("control-plane SIEM DR sink does not support checkpoint evidence verification")
				}
			}
			reconciler, reconcileErr := siem.NewCheckpointReconciler(taskRepo, siem.CheckpointReconcilerConfig{
				Endpoint:          cfg.SIEMReconcileEndpoint,
				BearerToken:       cfg.ControlSIEMToken,
				HTTPTimeout:       time.Duration(cfg.SIEMReconcileTimeoutMS) * time.Millisecond,
				PageSize:          cfg.SIEMReconcilePageSize,
				EvidenceVerifier:  evidenceVerifier,
				MinEvidenceCopies: cfg.SIEMReconcileMinEvidence,
				SignatureVerifier: checkpointVerifier,
				ExpectedReceiver:  cfg.SIEMReconcileReceiverID,
			})
			if reconcileErr != nil {
				log.Fatalf("init control-plane SIEM checkpoint reconciliation: %v", reconcileErr)
			}
			imported, reconcileErr := reconciler.Reconcile(ctx)
			if reconcileErr != nil {
				appmetrics.ControlPlaneSIEMReconcile.WithLabelValues("failed").Inc()
				log.Fatalf("reconcile control-plane SIEM receiver checkpoint: %v", reconcileErr)
			}
			appmetrics.ControlPlaneSIEMReconcile.WithLabelValues("success").Inc()
			if imported > 0 {
				log.Printf("reconciled %d control-plane SIEM receipts from receiver checkpoint", imported)
			}
		}
		deliveryWorker, err := siem.NewDeliveryWorker(taskRepo, siem.DeliveryConfig{
			Endpoint:                              cfg.ControlSIEMEndpoint,
			PrimaryReceiver:                       cfg.SIEMPrimaryReceiverID,
			PrimaryCheckpointEndpoint:             cfg.SIEMPrimaryCheckpoint,
			SecondaryEndpoint:                     cfg.SIEMSecondaryEndpoint,
			SecondaryReceiver:                     cfg.SIEMSecondaryReceiverID,
			SecondaryCheckpointEndpoint:           cfg.SIEMSecondaryCheckpoint,
			PrimaryTermEndpoint:                   cfg.SIEMPrimaryTermEndpoint,
			SecondaryTermEndpoint:                 cfg.SIEMSecondaryTermEndpoint,
			FailoverEnabled:                       cfg.SIEMFailoverEnabled,
			ShadowSecondary:                       cfg.SIEMShadowSecondary,
			StartOnSecondary:                      startOnSecondary,
			TermFencingEnabled:                    cfg.SIEMTermFencingEnabled,
			AntiEntropyEnabled:                    cfg.SIEMAntiEntropyEnabled,
			AntiEntropyInterval:                   time.Duration(cfg.SIEMAntiEntropyIntervalMS) * time.Millisecond,
			PromotionWorkflowEnabled:              cfg.SIEMPromotionWorkflow,
			PromotionIntentTTL:                    time.Duration(cfg.SIEMPromotionIntentTTLMS) * time.Millisecond,
			PromotionLease:                        time.Duration(cfg.SIEMPromotionLeaseMS) * time.Millisecond,
			PromotionApprovalQuorum:               cfg.SIEMPromotionApprovalQuorum,
			PromotionRolePolicy:                   cfg.SIEMPromotionRolePolicy,
			BreakGlassEnabled:                     cfg.SIEMBreakGlassEnabled,
			Governance:                            governance,
			GovernanceDebtPolicyEnabled:           cfg.SIEMGovernanceDebtPolicy,
			GovernanceMaxOutstanding:              cfg.SIEMGovernanceMaxOutstanding,
			GovernanceMaxAge:                      time.Duration(cfg.SIEMGovernanceMaxAgeMS) * time.Millisecond,
			GovernanceRemediationApprovalRequired: cfg.SIEMGovernanceRemediationApproval,
			GovernanceRemediationApprovalTTL:      time.Duration(cfg.SIEMGovernanceRemediationTTLMS) * time.Millisecond,
			GovernanceRemediationStaleTargetFence: cfg.SIEMGovernanceStaleTargetFence,
			RTOAlertThreshold:                     time.Duration(cfg.SIEMRTOAlertThresholdMS) * time.Millisecond,
			RPOPolicyEnabled:                      cfg.SIEMRPOPolicyEnabled,
			RPOMaxSequences:                       uint64(cfg.SIEMRPOMaxSequences),
			RPOGrace:                              time.Duration(cfg.SIEMRPOGraceMS) * time.Millisecond,
			CheckpointVerifier:                    checkpointVerifier,
			BearerToken:                           cfg.ControlSIEMToken,
			Interval:                              time.Duration(cfg.ControlSIEMIntervalMS) * time.Millisecond,
			BatchSize:                             cfg.ControlSIEMBatch,
			LeaseDuration:                         time.Duration(cfg.ControlSIEMLeaseMS) * time.Millisecond,
			MaxAttempts:                           cfg.ControlSIEMMaxAttempts,
			RetryBase:                             time.Duration(cfg.ControlSIEMRetryBaseMS) * time.Millisecond,
			RetryMax:                              time.Duration(cfg.ControlSIEMRetryMaxMS) * time.Millisecond,
			HTTPTimeout:                           time.Duration(cfg.ControlSIEMHTTPTimeoutMS) * time.Millisecond,
			Coordinator:                           deliveryCoordinator,
			LeadershipTTL:                         time.Duration(cfg.SIEMCoordLeaseMS) * time.Millisecond,
			EvidenceReplica:                       evidenceReplica,
		}, signer)
		if err != nil {
			log.Fatalf("init control-plane SIEM delivery: %v", err)
		}
		if cfg.SIEMTermFencingEnabled || cfg.SIEMAntiEntropyEnabled || cfg.SIEMPromotionWorkflow || cfg.SIEMRPOPolicyEnabled {
			siemReceiverControl = deliveryWorker
		}
		if cfg.SIEMTermFencingEnabled {
			if _, stateErr := deliveryWorker.EnsureReceiverRoutingState(ctx); stateErr != nil {
				log.Fatalf("restore control-plane SIEM receiver routing state: %v", stateErr)
			}
			status, statusErr := deliveryWorker.ReceiverStatus(ctx)
			if statusErr != nil {
				log.Fatalf("validate control-plane SIEM receiver term state: %v", statusErr)
			}
			if status.PrimaryReceiverTerm > status.Term || status.SecondaryReceiverTerm > status.Term {
				log.Fatalf("control-plane SIEM durable routing term=%d is behind receiver checkpoint terms primary=%d secondary=%d", status.Term, status.PrimaryReceiverTerm, status.SecondaryReceiverTerm)
			}
			activeTerm := status.PrimaryReceiverTerm
			if status.ActiveReceiver == cfg.SIEMSecondaryReceiverID {
				activeTerm = status.SecondaryReceiverTerm
			}
			if activeTerm != status.Term {
				log.Fatalf("control-plane SIEM active receiver=%s checkpoint term=%d does not match durable term=%d", status.ActiveReceiver, activeTerm, status.Term)
			}
			if cfg.SIEMPromotionWorkflow {
				_, historyReport, historyErr := deliveryWorker.ReceiverTermHistory(ctx, 1000)
				if historyErr != nil {
					log.Fatalf("verify control-plane SIEM receiver term history: %v", historyErr)
				}
				if !historyReport.Valid {
					log.Fatalf("control-plane SIEM receiver term history is invalid at term=%d: %s", historyReport.FailedTerm, historyReport.FailureCause)
				}
				if governance != nil {
					if _, anchorErr := deliveryWorker.EnsureTermAnchor(ctx); anchorErr != nil {
						log.Printf("defer control-plane SIEM receiver term anchoring to governance reconciler: %v", anchorErr)
					}
				}
			}
		}
		deliveryWorker.Start(ctx)
		if cfg.SIEMGCEnabled {
			monitor.StartControlPlaneSIEMHistoryLifecycle(
				ctx,
				taskRepo,
				time.Duration(cfg.SIEMGCIntervalMS)*time.Millisecond,
				time.Duration(cfg.SIEMDeliveredKeepMS)*time.Millisecond,
				time.Duration(cfg.SIEMDLQKeepMS)*time.Millisecond,
				cfg.SIEMGCBatch,
			)
		}
	}
	if cfg.ControlAuditRetention {
		monitor.StartControlPlaneAuditLifecycle(
			ctx,
			controlPlaneAudits,
			time.Duration(cfg.ControlAuditLifecycleMS)*time.Millisecond,
			time.Duration(cfg.ControlAuditRetentionMS)*time.Millisecond,
			cfg.ControlAuditRetainBatch,
		)
	}
	policyManager := service.NewAIRoutingPolicyManager(taskRepo, cfg.AIRoutingStrategy, cfg.AIDefaultOutputTokens)
	if err := policyManager.Init(ctx); err != nil {
		log.Fatalf("init AI routing policy: %v", err)
	}
	policyManager.Start(ctx, 2*time.Second)
	aiGovernance := service.NewAIGovernanceService(taskRepo, policyManager)
	budgetManager := service.NewAIBudgetManager(taskRepo, policyManager, cfg.AIBudgetDefaultReserveUSD)
	aiGovernance.ConfigureBudgetManager(budgetManager)
	if statsProvider, ok := taskQueue.(queue.StatsProvider); ok {
		monitor.StartQueueStats(ctx, statsProvider, time.Duration(cfg.QueueStatsIntervalMS)*time.Millisecond)
	}
	if cfg.QueueCompactionEnabled {
		if compactor, ok := taskQueue.(queue.Compactor); ok {
			monitor.StartQueueCompaction(
				ctx,
				compactor,
				time.Duration(cfg.QueueCompactionIntervalMS)*time.Millisecond,
				queue.CompactionPolicy{
					Retention:          time.Duration(cfg.QueueAckRetentionMS) * time.Millisecond,
					MaxRetainedEntries: int64(cfg.QueueAckRetentionMax),
				},
			)
		}
	}

	if cfg.DLQLifecycleEnabled {
		monitor.StartDeadLetterLifecycle(
			ctx,
			taskRepo,
			deadLetterService,
			time.Duration(cfg.DLQLifecycleIntervalMS)*time.Millisecond,
			monitor.DeadLetterLifecyclePolicy{
				ReplayedRetention:    time.Duration(cfg.DLQReplayedRetentionMS) * time.Millisecond,
				QuarantinedRetention: time.Duration(cfg.DLQQuarantineRetentionMS) * time.Millisecond,
				ReplayAuditRetention: time.Duration(cfg.DLQAuditRetentionMS) * time.Millisecond,
				MaxEntries:           cfg.DLQMaxEntries,
				BatchSize:            500,
				ArchiveBatchSize:     50,
				ArchiveMinCopies:     cfg.DLQArchiveMinCopies,
			},
		)
	}

	if cfg.DLQArchiveEnabled && cfg.DLQObjLifecycleEnabled {
		monitor.StartDeadLetterArchiveObjectLifecycle(
			ctx,
			taskRepo,
			deadLetterService,
			time.Duration(cfg.DLQObjLifecycleIntervalMS)*time.Millisecond,
			monitor.DeadLetterArchiveObjectPolicy{
				ObjectRetention: time.Duration(cfg.DLQObjRetentionMS) * time.Millisecond,
				OrphanGrace:     time.Duration(cfg.DLQOrphanGraceMS) * time.Millisecond,
				DrillInterval:   time.Duration(cfg.DLQDrillIntervalMS) * time.Millisecond,
				GCBatchSize:     cfg.DLQObjGCBatch,
				OrphanBatchSize: cfg.DLQOrphanBatch,
				DrillBatchSize:  cfg.DLQDrillBatch,
			},
		)
	}

	var exec executor.Executor
	var executorReady handler.ReadinessCheck
	switch strings.ToLower(strings.TrimSpace(cfg.ExecutionBackend)) {
	case "local":
		var distributedCoordinator executor.DistributedConcurrencyCoordinator
		coordinator := executor.NewRedisConcurrencyCoordinator(rdb, cfg.AIConcurrencyRedisPrefix, 25*time.Millisecond)
		var tenantCoordinator executor.DistributedTenantConcurrencyCoordinator = coordinator
		if cfg.AIGlobalConcurrencyLimit > 0 || cfg.AIFallbackGlobalLimit > 0 || cfg.AITenantQuotaEnabled {
			if cfg.AIGlobalConcurrencyLimit > 0 || cfg.AIFallbackGlobalLimit > 0 {
				distributedCoordinator = coordinator
			}
		}
		exec = executor.NewTaskExecutorWithRouting(
			time.Duration(cfg.AITimeoutMS)*time.Millisecond,
			executor.AIRoutingConfig{
				Strategy:            cfg.AIRoutingStrategy,
				CircuitFailures:     cfg.AIProviderCircuitFailures,
				CircuitOpenFor:      time.Duration(cfg.AIProviderCircuitOpenMS) * time.Millisecond,
				DefaultOutputTokens: cfg.AIDefaultOutputTokens,
				SLO: executor.AISLOConfig{
					Realtime: executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLORealtimeQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLORealtimeTimeoutMS) * time.Millisecond},
					Standard: executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLOStandardQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLOStandardTimeoutMS) * time.Millisecond},
					Batch:    executor.AISLOPolicy{QueueWait: time.Duration(cfg.AISLOBatchQueueMS) * time.Millisecond, Timeout: time.Duration(cfg.AISLOBatchTimeoutMS) * time.Millisecond},
				},
				FairScheduling: executor.AIFairSchedulingConfig{
					Enabled:                 cfg.AITenantFairScheduling,
					MaxConsecutivePerTenant: cfg.AITenantMaxConsecutive,
					StarvationAfter:         time.Duration(cfg.AITenantStarvationMS) * time.Millisecond,
				},
				DistributedCoordinator: distributedCoordinator,
				TenantCoordinator:      tenantCoordinator,
				TenantQuota: executor.AITenantDistributedQuotaConfig{
					Enabled:      cfg.AITenantQuotaEnabled,
					DefaultLimit: cfg.AITenantQuotaLimit,
					Overrides:    executor.ParseAITenantConcurrencyOverrides(cfg.AITenantQuotaOverrides),
				},
				DistributedLeaseTTL: time.Duration(cfg.AIDistributedLeaseMS) * time.Millisecond,
				LoadShedding: executor.AILoadSheddingConfig{
					Enabled:               cfg.AIAdaptiveLoadShedding,
					DegradedQueueDepth:    cfg.AIPressureQueueWarn,
					OverloadedQueueDepth:  cfg.AIPressureQueueHigh,
					DegradedFailureRate:   cfg.AIPressureFailWarn,
					OverloadedFailureRate: cfg.AIPressureFailHigh,
					DegradedLatency:       time.Duration(cfg.AIPressureLatencyWarnMS) * time.Millisecond,
					OverloadedLatency:     time.Duration(cfg.AIPressureLatencyHighMS) * time.Millisecond,
				},
				Hedge: executor.AIHedgeConfig{
					Enabled:         cfg.AIHedgeEnabled,
					Delay:           time.Duration(cfg.AIHedgeDelayMS) * time.Millisecond,
					CancelWait:      time.Duration(cfg.AIHedgeCancelWaitMS) * time.Millisecond,
					MaxExtraCostUSD: cfg.AIHedgeMaxExtraCostUSD,
				},
			},
			executor.AIProviderConfig{
				Name: cfg.AIProvider, Kind: cfg.AIProviderKind, Endpoint: cfg.AIEndpoint, BearerToken: cfg.AIBearerToken,
				Model: cfg.AIModel, Capabilities: executor.ParseAICapabilities(cfg.AICapabilities),
				InputCostPer1M: cfg.AIInputCostPer1M, OutputCostPer1M: cfg.AIOutputCostPer1M, MaxConcurrency: cfg.AIMaxConcurrency, GlobalMaxConcurrency: cfg.AIGlobalConcurrencyLimit,
			},
			executor.AIProviderConfig{
				Name: cfg.AIFallbackProvider, Kind: cfg.AIFallbackKind, Endpoint: cfg.AIFallbackEndpoint, BearerToken: cfg.AIFallbackToken,
				Model: cfg.AIFallbackModel, Capabilities: executor.ParseAICapabilities(cfg.AIFallbackCaps),
				InputCostPer1M: cfg.AIFallbackInputCostPer1M, OutputCostPer1M: cfg.AIFallbackOutputCostPer1M, MaxConcurrency: cfg.AIFallbackMaxConcurrency, GlobalMaxConcurrency: cfg.AIFallbackGlobalLimit,
			},
		)
	case "grpc":
		grpcExec, err := executor.NewGRPCExecutor(cfg.ExecutorGRPCTarget, time.Duration(cfg.ExecutorTimeoutMS)*time.Millisecond)
		if err != nil {
			log.Fatalf("create grpc executor: %v", err)
		}
		grpcExec.ConfigureCircuitBreaker(cfg.CircuitFailures, time.Duration(cfg.CircuitOpenMS)*time.Millisecond)
		grpcExec.ConfigureTaskTimeout(
			executor.AITaskType,
			time.Duration(cfg.AITimeoutMS+5000)*time.Millisecond,
		)
		defer grpcExec.Close()
		exec = grpcExec
		executorReady = grpcExec.Ready
	default:
		log.Fatalf("unsupported EXECUTION_BACKEND %q", cfg.ExecutionBackend)
	}
	if verificationConfig != nil {
		router := executor.NewRouter(exec)
		router.Register(domain.VerificationTaskType, executor.NewVerificationExecutor(verificationConfig, taskRepo))
		exec = router
	}
	runtimeReady := func(ctx context.Context) error {
		if draining.Load() {
			return handler.ErrDraining
		}
		if executorReady != nil {
			return executorReady(ctx)
		}
		return nil
	}
	leaseDuration := time.Duration(cfg.StaleRunSec) * time.Second
	pool := worker.NewPool(cfg.WorkerCount, taskQueue, taskRepo, exec, cfg.MaxRetries, leaseDuration)
	pool.ConfigureAIRoutingPolicyProvider(policyManager)
	pool.ConfigureAIBudgetAdmission(budgetManager)
	pool.Start(ctx)
	outboxDispatcher := outbox.New(taskRepo, taskQueue, time.Duration(cfg.OutboxIntervalMS)*time.Millisecond)
	outboxDispatcher.ConfigureEnqueueLease(time.Duration(cfg.EnqueueLeaseMS) * time.Millisecond)
	outboxDispatcher.Start(ctx)
	reconcileRunner := reconciler.New(
		taskRepo,
		taskQueue,
		time.Duration(cfg.ReconcileSec)*time.Second,
		time.Duration(cfg.StaleRunSec)*time.Second,
	)
	reconcileRunner.ConfigureEnqueueLease(time.Duration(cfg.EnqueueLeaseMS) * time.Millisecond)
	if cfg.ReconcileDistributed {
		reconcileRunner.ConfigureCoordinator(
			reconciler.NewRedisCoordinator(rdb, cfg.ReconcileLockKey),
			time.Duration(cfg.ReconcileLeaseMS)*time.Millisecond,
		)
	}
	reconcileRunner.Start(reconcileCtx)

	controlPlaneAuthConfig := handler.ControlPlaneAuthConfig{
		Enabled:                     cfg.ControlPlaneAuthEnabled,
		BearerToken:                 cfg.ControlPlaneBearerToken,
		PreviousBearerToken:         cfg.ControlPlanePrevToken,
		AdditionalBearerCredentials: cfg.ControlPlaneExtraCredentials,
		OperatorID:                  cfg.ControlPlaneOperatorID,
		PreviousOperatorID:          cfg.ControlPlanePreviousID,
		Mode:                        cfg.ControlPlaneAuthMode,
		OIDCIssuer:                  cfg.ControlPlaneOIDCIssuer,
		OIDCAudience:                cfg.ControlPlaneOIDCAudience,
		OIDCJWKSURL:                 cfg.ControlPlaneOIDCJWKSURL,
		OIDCGroupsClaim:             cfg.ControlPlaneOIDCGroups,
		OIDCRoleMappings:            cfg.ControlPlaneOIDCRoleMap,
		OIDCJWKSCacheTTL:            time.Duration(cfg.ControlPlaneOIDCJWKSTTLMS) * time.Millisecond,
		OIDCClockSkew:               time.Duration(cfg.ControlPlaneOIDCSkewMS) * time.Millisecond,
		OIDCRBACEnabled:             cfg.ControlPlaneOIDCRBAC,
		CloudTenantAuthzEnabled:     cfg.CloudTenantAuthzEnabled,
	}
	controlPlaneAuditConfig := handler.ControlPlaneAuditConfig{Enabled: cfg.ControlPlaneAuditEnabled}
	router := handler.NewRouterWithSecurity(
		taskService,
		db,
		rdb,
		cfg.RateLimitRPS,
		cfg.RateLimitBurst,
		cfg.AccessLog,
		runtimeReady,
		aiGovernance,
		deadLetterService,
		controlPlaneAuthConfig,
		controlPlaneAudits,
		controlPlaneAuditConfig,
		siemReceiverControl,
	)
	handler.RegisterVerificationRoutes(router, service.NewVerificationService(taskRepo, verificationConfig), controlPlaneAuthConfig)
	handler.RegisterCloudControlRoutes(
		router,
		cloudResourceService,
		controlPlaneAuthConfig,
		controlPlaneAudits,
		controlPlaneAuditConfig,
	)
	handler.RegisterCloudWorkflowRoutes(
		router,
		cloudWorkflowService,
		controlPlaneAuthConfig,
		controlPlaneAudits,
		controlPlaneAuditConfig,
	)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	pprofServer := &http.Server{
		Addr:              cfg.PprofAddr,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("taskflow listening on %s with %d workers", cfg.HTTPAddr, cfg.WorkerCount)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	go func() {
		log.Printf("pprof listening on %s", cfg.PprofAddr)
		if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("pprof server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	draining.Store(true)
	appmetrics.ProcessDraining.Set(1)
	reconcileCancel()
	pool.BeginDrain()
	log.Printf("taskflow draining: intake fenced, active=%d", pool.Active())

	if cfg.DrainFenceMS > 0 {
		time.Sleep(time.Duration(cfg.DrainFenceMS) * time.Millisecond)
	}

	poolDone := make(chan struct{})
	go func() {
		pool.Wait()
		close(poolDone)
	}()
	drainTimer := time.NewTimer(time.Duration(cfg.DrainTimeoutMS) * time.Millisecond)
	select {
	case <-poolDone:
		if !drainTimer.Stop() {
			<-drainTimer.C
		}
		appmetrics.DrainOutcomes.WithLabelValues("taskflow", "graceful").Inc()
		log.Printf("taskflow drain completed")
	case <-drainTimer.C:
		appmetrics.DrainOutcomes.WithLabelValues("taskflow", "timeout").Inc()
		log.Printf("taskflow drain timeout with active=%d; canceling execution context", pool.Active())
		cancel()
		forceTimer := time.NewTimer(3 * time.Second)
		select {
		case <-poolDone:
			if !forceTimer.Stop() {
				<-forceTimer.C
			}
		case <-forceTimer.C:
			log.Printf("taskflow forced shutdown with active=%d; task leases remain recoverable", pool.Active())
		}
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown: %v", err)
	}
	if err := pprofServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("pprof shutdown: %v", err)
	}
}
