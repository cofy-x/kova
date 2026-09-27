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
  --platform linux/amd64 \
  --repository registry.example.com/team/kova-sources:request-123 \
  ./image
```

Kova also accepts an immutable HTTPS archive URL without user information, query credentials, or fragments when the caller supplies the expected SHA-256 content digest.
OCI bundle retention and garbage collection belong to the registry or caller.
The source contract permits at most 512 MiB of exact zip bytes, 2 GiB of total expanded ZIP member bytes, and 100,000 ZIP entries.
These fixed limits apply to local source inspection and push, HTTPS and OCI fetches, runner uploads, and extraction.
Kova counts bytes while reading and extracting as well as checking ZIP headers; a rejected source is reported as `InvalidSource` during source inspection or fetch, before any image push.

Each request has 1–100 logical targets. Every target is an object containing one unique, explicitly tagged OCI push destination and exactly one supported platform: `linux/amd64` or `linux/arm64`.
Digest-only destinations and free-form platform labels are rejected.
The `_nydus_v3` tag suffix is reserved for Kova's derived Nydus output and is rejected on logical targets, preventing OCI and Nydus concrete output collisions.
With `format=both`, status can contain at most 200 concrete outputs.
Requested concurrency is between 1 and 100 and cannot exceed the logical target count.

After a source is fetched and digest-verified, the controller inspects its metadata in the runner Pod.
The normalized source `(target, platform)` set must exactly match `spec.targets` before the build request is sent to BuildKit.
Order is irrelevant, but missing, extra, mismatched-platform, invalid, and duplicate targets are deterministic failures with no image push.

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
  --platform linux/amd64 \
  --format oci \
  --idempotency-key request-123 \
  ./image
```

Submit an already published source without uploading local bytes:

```bash
kova job submit \
  --source-digest sha256:<content-digest> \
  --target registry.example.com/team/image:dev \
  --platform linux/amd64 \
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
from kova_client import ClientConfig, CreateBuildRequest, JobStatus, KovaClient, Platform, TargetSpec

config = ClientConfig.from_env()
with KovaClient(config) as kova:
    job = kova.create_build(
        CreateBuildRequest(
            source_uri="oci://registry.example.com/team/sources@sha256:<manifest-digest>",
            source_digest="sha256:<source-content-digest>",
            targets=(TargetSpec("registry.example.com/team/seed:build-123", Platform.LINUX_AMD64),),
            concurrency=1,
            idempotency_key="build-123",
        )
    )
    terminal = kova.wait_build(job.id, timeout=600)
    if terminal.status is JobStatus.SUCCEEDED:
        results = kova.get_results(job.id)
        for output in results.outputs:
            print(output.platform, output.immutable_ref, output.manifest_digest)
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
The [executable Python and Go Service SDK examples](../examples/service-sdk/README.md) demonstrate the complete source URI and digest to receipt flow with one shared environment contract. The [seed build receipt reference shape](examples/seed-build-receipt-v1.json) records recipe identity, terminal status, target role, platform, format, image, digest, and immutable reference. The caller owns this receipt; Kova does not provide a long-term receipt store.

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
		Targets:        []apiv1.TargetSpec{{Target: "registry.example.com/team/seed:build-123", Platform: apiv1.PlatformLinuxAMD64}},
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
		fmt.Println(output.Platform, output.ImmutableRef, output.ManifestDigest)
	}
}
```

Every network method accepts `context.Context`, and `WaitBuild` stops when that context is cancelled. It retries only retryable failures from the idempotent status endpoint and honors `Retry-After` before polling again.
`client.Config` accepts a bearer token, kubeconfig, CA file, insecure TLS mode for controlled development, an injected `http.Client`, and an optional response-size bound.
Responses are bounded to 64 MiB by default so a faulty endpoint cannot cause unbounded client allocation.
The public `Version` and `Ready` probes do not attach a bearer token configured directly on the client.
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

The [executable Service SDK examples](../examples/service-sdk/README.md) use only `pkg/api/v1` and `pkg/client`, preserve all verified outputs in stable order, and demonstrate typed API errors, bounded waiting, terminal failure handling, and caller-owned receipt persistence.

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
    "targets": [{
      "target": "registry.example.com/team/image:dev",
      "platform": "linux/amd64"
    }],
    "format": "oci",
    "concurrency": 1,
    "timeout": 600,
    "idempotency_key": "request-123"
  }'
```

The first request returns `202 Accepted` after an atomic queue-intent reservation.
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
After the runner completes, `verifying` is a durable nonterminal state; the response includes verification start/deadline/next-attempt times, attempt count, last error, and pending/succeeded/failed output counts.
They do not expose runner Pod names, Kubernetes namespaces, or internal BuildKit addresses.
List pages are limited to 500 jobs, and log requests are limited to the last 10,000 lines.

Each successful output contains `format`, `platform`, the mutable pushed `image` tag, `manifest_digest`, and a server-derived `immutable_ref`.
The Service removes the explicit tag, preserves registry ports and nested repositories, validates the SHA-256 digest, and returns a canonical `repository@sha256:...` reference.
Clients must not construct this reference themselves.
Registry descriptor checks use bounded parallelism.
Kova resolves the digest-pinned single-platform manifest, reads its image configuration, and requires its OS and architecture to match the request; it never verifies platform through the mutable tag.
For OCI outputs, Kova records the digest returned by that build's BuildKit push.
For Nydus outputs, the source-pinned Nydusify converter records the descriptor digest after that build's target push succeeds.
Both formats fail verification if the push metadata omits a valid digest; a later lookup of the mutable tag cannot replace that digest.
Runner completion and exact push receipts are persisted before registry verification. Each attempt has a 10-second default deadline and at most 16 pending outputs, with no more than four registry requests in flight. Transient export/registry failures retry with capped backoff inside a separate five-minute default verification window; an immutable digest or platform mismatch fails immediately. The controller never submits a completed runner again, including after restart or leader handoff.
Only two controller reconciles perform verification I/O at once under the default four-reconcile setting, leaving admission capacity for other jobs. The runner Pod remains available during verification and is removed before terminal status; cancellation or deletion also removes it. The Kubernetes active deadline includes the verification window, while the build-execution deadline still ends at `maxBuildDuration`.
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
  "message": "queue limit is reached",
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
| `queue_admission_pending` | 503 | True; one CR Create has an unknown or not-yet-observed outcome. Retry with the **same** idempotency key or inspect the `X-Kova-Build-ID`; do not submit a new key to recover this intent. |
| `logs_unavailable` | 404 or 410 | True before a runner starts and false after terminal cleanup begins. |
| `internal` | 500 or 503 | True only for transient service failures; false for deterministic service configuration or stored-contract failures. Mutating retries still require an idempotency key. |

Non-administrative responses do not include raw Kubernetes, runner, Pod, registry credential, or implementation errors.
Detailed implementation failures remain in operator-controlled logs and Kubernetes status rather than the public error response.

HTTP submissions reserve in a single CAS queue ledger before CR creation. It applies both `maxQueuedJobs` (global, at most 1000) and `maxQueuedJobsPerRequester` across Service replicas. Direct/admin-created CRs are **outside** these HTTP queue limits, but their runner admission still uses the active hard limit. Unknown CR Create results keep their intent; the authenticated caller can query `GET /v1/builds/<id>` for a pending error when no CR is visible. Both SDKs expose the Build ID on API errors. A pending intent may require operator recovery and must not be released merely because the CR is absent. The exact safety conditions and upgrade barrier are in the [Service admission design](service-admission-design.md).
If an earlier runner Pod Create has an unknown outcome, the build reports `recovery_required=true` while its capacity remains reserved; an operator must resolve the recorded attempt before that slot can be reused.

## Helm Configuration

Enable the Service with TokenReview:

```yaml
serviceDaemon:
  enabled: true
  authentication:
    mode: tokenreview
  maxActiveJobs: 20
  maxActiveJobsPerRequester: 4
  maxQueuedJobs: 1000
  maxQueuedJobsPerRequester: 100
  workerSlots: 40
  controllerConcurrency: 4
  pollRetryWindow: 1m
  maxBuildDuration: 2h
  verificationAttemptTimeout: 10s
  verificationWindow: 5m

worker:
  platform: linux/amd64
```

The Service retries temporary runner status and source-inspect transport errors with backoff while checking the runner Pod, for at most `pollRetryWindow`.
`maxBuildDuration` is the Service-owned limit from runner admission through completion; it applies even when a request sets the per-target `timeout` to `0`.
At the limit the controller cancels the runner, deletes its Pod, and reports a failed build so its active slot can be reused.
The runner Pod also has a Kubernetes active deadline, which stops a hung build if the controller is temporarily unavailable.
When the controller reconciles after the deadline, it first makes a bounded status check; a reachable terminal runner state is processed and verified, while an active or unobservable runner is timed out.
The separate verification window starts when a completed runner is durably observed. If it expires, pending outputs fail with `result_verification_failed`; verified digests remain available as partial results. Adjust the window for registry consistency and the number of concrete outputs, not to extend build execution.

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

One chart release provides one platform-specific worker pool and selects its nodes with the standard `kubernetes.io/os` and `kubernetes.io/arch` labels. To serve both architectures, install another worker release for the second platform and configure the Service release with explicit addresses:

```yaml
serviceDaemon:
  buildkitPlatformAddrs:
    linux/amd64: tcp://kova-amd64.kova.svc:9094
    linux/arm64: tcp://kova-arm64.kova.svc:9094
```

If a request names a supported platform without a configured pool, it terminates with `worker_platform_unavailable`. Kova does not infer a target platform from the controller or runner node.

The Service needs no object store, shared filesystem, or RWX PVC.
Each runner uses job-local `emptyDir` storage for the verified source bundle.
