# TaskFlow Cloud API: Go client adoption guide

The public `pkg/cloudclient` package is a versioned HTTP client for Go
services integrating with the TaskFlow cloud control plane. The contract is
tracked in [`../api/cloud-openapi.json`](../api/cloud-openapi.json).

It accepts a rotating token provider, an explicitly configured HTTP transport,
and context cancellation. All writes require a client-generated stable
`Idempotency-Key` and **do not retry automatically**. A retry is a separate
caller decision after examining whether the previous outcome is known.

## Use from another Go service

```go
package myservice

import (
    "context"
    "net/http"
    "time"

    "github.com/yfj898/taskflow-go/pkg/cloudclient"
)

func allocate(ctx context.Context, signedToken string) error {
    c, err := cloudclient.New("https://taskflow.example.com", func(context.Context) (string, error) {
        return signedToken, nil
    }, &http.Client{Timeout: 15 * time.Second})
    if err != nil { return err }

    // Bind the idempotency key to the business order, not a random value
    // regenerated on each HTTP retry.
    order, err := c.CreateResource(ctx, cloudclient.CreateResource{
        TenantID: "team-platform",
        Name: "preview-env-a",
        Spec: map[string]any{"image": "nginx:1.27", "replicas": 1,
            "cpu": "200m", "memory": "256Mi"},
    }, "platform-order-20261009-001")
    if err != nil { return err }

    _, err = c.GetResource(ctx, order.Resource.ID)
    return err
}
```

This is a client example; it is not proof of external organizational adoption.
The signed token is issued by the deployment's trusted OIDC identity provider,
not by the SDK. Its `tenant_id`/`tenant_ids` claims grant authorization, and
the endpoint must be protected by TLS outside a local test environment.

## HTTP behavior

- 401: missing, expired, invalid or unacceptable OIDC JWT.
- 403: authorized identity but tenant path/body/query scope forbidden.
- 404: nonexistent or inaccessible resource ID (do not disclose existence).
- 409: conflicting idempotency key, active workflow or quota contention.
- 429/503: overload or temporarily unavailable; decide on retries with stable
  keys and a bounded deadline, never blind retries of a write.

The SDK returns typed `*cloudclient.APIError` with status, request ID and
server error description. It limits response size to 4 MiB and propagates
caller context; workflow wait only polls and never forces reconciliation.

## Verification

```
go test -count=1 ./pkg/cloudclient
```

`TestCloudTenantRouterMySQLIntegration` also creates a standalone HTTP server
with genuine JWT signature verification and real MySQL and exercises the SDK
through that entire path. Provider side effects are intentionally mocked:
this test does not create actual Kubernetes resources.
