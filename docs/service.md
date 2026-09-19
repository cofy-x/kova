# Kova Service

`kova-controller service` is the authenticated HTTP gateway and `KovaBuild` controller.
It validates immutable source contracts, admits builds by capacity, creates one short-lived runner Pod per build, and verifies pushed image manifest digests.

## Authentication and Authorization

Every `/v1/*` request requires an explicit authentication mode:

- `tokenreview` validates bearer tokens with Kubernetes and is the chart default;
- `static` compares a token from `KOVA_SERVICE_AUTH_TOKEN` and suits isolated automation or quick starts;
- `unsafe-none` disables authentication and is restricted to isolated development clusters.

TokenReview mode submits SubjectAccessReview requests for the virtual `servicebuilds.kova.cofy.dev` resource.
Only the controller ServiceAccount writes actual `KovaBuild` resources, so callers cannot bypass ownership or idempotency through the public API.
The chart creates unbound `kova-service-submitter` and `kova-service-admin` Roles; each environment owns their RoleBindings.

`/healthz` is unauthenticated liveness.
`/readyz` verifies Kubernetes API access.
`/version` reports Service API and build provenance without credentials.

## Immutable Sources

The preferred source is an OCI bundle published with `kova source push`.
The command creates a deterministic zip, publishes one custom OCI layer, and returns both a manifest-pinned `oci://` URI and the SHA-256 digest of the zip bytes:

```bash
kova source push \
  --target registry.example.com/team/image:dev \
  --repository registry.example.com/team/kova-sources:request-123 \
  ./image
```

Kova also accepts an immutable HTTPS archive URL without user information, query credentials, or fragments when the caller supplies the expected SHA-256 content digest.
OCI bundle retention and garbage collection belong to the registry or caller.

Each request has 1–100 logical targets.
Targets must be unique, explicitly tagged OCI push destinations; digest-only destinations are rejected.
With `format=both`, status can contain at most 200 concrete outputs.
Requested concurrency is between 1 and 100 and cannot exceed the logical target count.

After a source is fetched and digest-verified, the controller inspects its metadata in the runner Pod.
The normalized source target set must exactly match `spec.targets` before the build request is sent to BuildKit.
Order is irrelevant, but missing, extra, invalid, and duplicate targets are deterministic failures with no image push.

## CLI Workflow

Create a Service context and keep bearer tokens in the process environment:

```bash
export KOVA_SERVICE_TOKEN=REPLACE_WITH_TOKEN
kubectl -n kova port-forward service/kova-service 8080:8080
kova ctx set --mode service --service-url http://127.0.0.1:8080 --use service
kova doctor
```

Submit a local context through the public source contract by naming an OCI source repository:

```bash
kova job submit \
  --source-repository registry.example.com/team/kova-sources:request-123 \
  --target registry.example.com/team/image:dev \
  --format oci \
  --idempotency-key request-123 \
  ./image
```

Submit an already published source without uploading local bytes:

```bash
kova job submit \
  --source-digest sha256:<content-digest> \
  --target registry.example.com/team/image:dev \
  oci://registry.example.com/team/kova-sources@sha256:<manifest-digest>
```

Manage the build with:

```bash
kova job list
kova job get <job-id>
kova job logs <job-id> --tail 100
kova job wait <job-id> --timeout 10m
kova job results <job-id>
kova job cancel <job-id>
```

Logs are available only while the runner Pod is active.
The results endpoint returns the source identity and verified image outputs; it does not return an object-store URI.

## Python SDK

The official Python distribution is `kova-client`, with the import package `kova_client`.
The distribution name makes its narrow HTTP client role explicit and avoids conflating the package with the Kova service and CLI.
Both `KovaClient` and `AsyncKovaClient` implement the same OpenAPI v1 operations without importing Kubernetes, Kova Go internals, Axern, or Axrun types.

```bash
python -m pip install kova-client
```

```python
from kova_client import ClientConfig, CreateBuildRequest, JobStatus, KovaClient

config = ClientConfig.from_env()
with KovaClient(config) as kova:
    job = kova.create_build(
        CreateBuildRequest(
            source_uri="oci://registry.example.com/team/sources@sha256:<manifest-digest>",
            source_digest="sha256:<source-content-digest>",
            targets=("registry.example.com/team/seed:build-123",),
            concurrency=1,
            idempotency_key="build-123",
        )
    )
    terminal = kova.wait_build(job.id, timeout=600)
    if terminal.status is JobStatus.SUCCEEDED:
        results = kova.get_results(job.id)
        for output in results.outputs:
            print(output.immutable_ref, output.manifest_digest)
```

Constructors are explicit and do not inspect a home directory or environment variables.
`ClientConfig.from_env()` is the opt-in environment adapter for `KOVA_SERVICE_URL`, `KOVA_SERVICE_TOKEN`, `KOVA_SERVICE_CA_FILE`, and `KOVA_SERVICE_INSECURE`.
Tokens are excluded from configuration representations and are never included in SDK exceptions or logs.
The unauthenticated `version` and `ready` calls do not send the configured bearer token.

`create_build` performs exactly one HTTP submission and never retries automatically.
`wait_build` only polls the idempotent build-status endpoint, stops on any terminal status, respects retryable API responses and `Retry-After`, and accepts a timeout plus a caller-owned cancellation event.
External cancellation of an async task propagates normally.
`KovaAPIError` exposes `status_code`, stable `code`, safe `message`, `retryable`, and `retry_after` as a `datetime.timedelta` when supplied.

The SDK returns `immutable_ref` exactly as supplied and validated by the Service; it never reconstructs an immutable reference from the mutable tag.
The caller owns retry policy and must persist source identity, build ID, manifest digest, and immutable reference before the terminal job TTL expires.
The [caller-owned receipt example](../examples/python-service-receipt.py) demonstrates the complete source URI and digest to receipt flow without adding receipt storage to Kova.

## Go SDK

The public Go contract is `github.com/cofy-x/kova/pkg/api/v1` and the public client is `github.com/cofy-x/kova/pkg/client`.
The Kova CLI uses this same client implementation.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	"github.com/cofy-x/kova/pkg/client"
)

func main() {
	ctx := context.Background()
	kova, err := client.New(client.Config{
		BaseURL: "https://kova.example.com",
		Token:   os.Getenv("KOVA_SERVICE_TOKEN"),
	})
	if err != nil {
		panic(err)
	}
	if err := kova.CheckCompatible(ctx); err != nil {
		panic(err)
	}
	job, err := kova.CreateBuild(ctx, apiv1.CreateBuildRequest{
		SourceURI:      "oci://registry.example.com/team/sources@sha256:<manifest-digest>",
		SourceDigest:   "sha256:<source-content-digest>",
		Targets:        []string{"registry.example.com/team/seed:build-123"},
		Format:         "oci",
		Concurrency:    1,
		IdempotencyKey: "build-123",
	})
	if err != nil {
		panic(err)
	}
	if _, err := kova.WaitBuild(ctx, job.ID, 2*time.Second); err != nil {
		panic(err)
	}
	results, err := kova.GetResults(ctx, job.ID)
	if err != nil {
		panic(err)
	}
	for _, output := range results.Outputs {
		fmt.Println(output.ImmutableRef, output.ManifestDigest)
	}
}
```

Every network method accepts `context.Context`, and `WaitBuild` stops when that context is cancelled.
`client.Config` accepts a bearer token, kubeconfig, CA file, insecure TLS mode for controlled development, an injected `http.Client`, and an optional response-size bound.
Responses are bounded to 64 MiB by default so a faulty endpoint cannot cause unbounded client allocation.
Compatibility checks are explicit; `CreateBuild` does not add a hidden `/version` request before submission.
The SDK does not automatically retry `CreateBuild`, cancellation, or any other mutating request.
Callers use a stable idempotency key when a submission may need to be retried safely.

An HTTP failure can be inspected with `errors.As` into `*client.APIError`.
It exposes the HTTP status, stable error code, safe message, retryable flag, and parsed `Retry-After` duration when the server supplied one.
The client never includes the bearer token in returned errors.

The SDK is an execution-plane client, not a workflow engine.
Terminal Kova jobs have a configured TTL and are removed after it expires.
Before then, callers must persist the immutable source URI and digest, Kova build ID, manifest digest, and `immutable_ref` in their own durable system.
The manifest digest and `immutable_ref` are the success facts; an image tag or transient runner log is not.

## HTTP API

The stable HTTP v1 surface is described by the [OpenAPI 3.1 Service contract](../api/openapi.yaml).

Create requests use JSON:

```bash
curl -sS -X POST "$BASE/v1/builds" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data '{
    "source_uri": "oci://registry.example.com/team/kova-sources@sha256:<manifest-digest>",
    "source_digest": "sha256:<content-digest>",
    "targets": ["registry.example.com/team/image:dev"],
    "format": "oci",
    "concurrency": 1,
    "timeout": 600,
    "idempotency_key": "request-123"
  }'
```

The first request returns `202 Accepted`.
Repeating the same caller-scoped idempotency key with identical inputs returns the existing build; changing any immutable input returns `409 Conflict`.
Unknown fields and mutable source references are rejected.

Query and control endpoints are:

```text
GET  /v1/builds
GET  /v1/builds/<id>
GET  /v1/builds/<id>/results
GET  /v1/builds/<id>/logs?tail_lines=100
POST /v1/builds/<id>/cancel
```

Job responses contain the public execution state and a stable `failure_code` for terminal failures.
They do not expose runner Pod names, Kubernetes namespaces, or internal BuildKit addresses.
List pages are limited to 500 jobs, and log requests are limited to the last 10,000 lines.

Each successful output contains `format`, the mutable pushed `image` tag, `manifest_digest`, and a server-derived `immutable_ref`.
The Service removes the explicit tag, preserves registry ports and nested repositories, validates the SHA-256 digest, and returns a canonical `repository@sha256:...` reference.
Clients must not construct this reference themselves.
Registry descriptor checks use bounded parallelism.
If one of several registries fails, the job is `Failed` while already verified output digests remain in status.
Registry pushes are not transactional and Kova does not roll them back.

A caller can retry safely by creating a new request with the same immutable source URI, source digest, targets, and build options.
Kova does not resume a failed build internally.
Workloads above 100 logical targets must be split by the caller into several bounded builds.

## Error Contract

All Service API failures use a structured response:

```json
{
  "code": "queue_capacity_exceeded",
  "message": "requester queue limit is reached",
  "retryable": true
}
```

| Code | HTTP status | Retryable semantics |
| --- | --- | --- |
| `invalid_request` | 400 or 405 | False; change the request before retrying. |
| `unauthenticated` | 401 | False; provide valid credentials. |
| `forbidden` | 403 | False; change caller authorization. |
| `not_found` | 404 | False; the job may never have existed or its TTL may have expired. |
| `conflict` | 409 | False; the idempotency key is bound to different immutable inputs. |
| `queue_capacity_exceeded` | 429 | True; respect `Retry-After` before making a new idempotent submission attempt. |
| `logs_unavailable` | 404 or 410 | True before a runner starts and false after terminal cleanup begins. |
| `internal` | 500 or 503 | True only for transient service failures; false for deterministic service configuration or stored-contract failures. Mutating retries still require an idempotency key. |

Non-administrative responses do not include raw Kubernetes, runner, Pod, registry credential, or implementation errors.
Detailed implementation failures remain in operator-controlled logs and Kubernetes status rather than the public error response.

## Helm Configuration

Enable the Service with TokenReview:

```yaml
serviceDaemon:
  enabled: true
  authentication:
    mode: tokenreview
  maxActiveJobs: 20
  maxActiveJobsPerRequester: 4
  maxQueuedJobsPerRequester: 100
  workerSlots: 40
  controllerConcurrency: 4
```

Registry credentials are the only storage credentials needed by Kova.
The same Docker config can authorize source pulls, output pushes, and controller-side manifest verification:

```yaml
imagePullSecrets:
  create: false
  name: kova-registry

serviceDaemon:
  runnerImagePullSecret: kova-registry
  registrySecret: kova-registry
```

Different targets may name different registries if the supplied Docker config, network policy, and TLS configuration cover all of them.
`serviceDaemon.registryPlainHTTP` explicitly lists development registries without TLS.
Production registries should use HTTPS.

The Service needs no object store, shared filesystem, or RWX PVC.
Each runner uses job-local `emptyDir` storage for the verified source bundle.
