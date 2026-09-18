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

Kova also accepts an immutable HTTPS archive URL when the caller supplies the expected SHA-256 content digest.
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

## HTTP API

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
POST /v1/builds/<id>/export
POST /v1/builds/<id>/preheat
```

Each successful output is the tuple `(format, image, manifest_digest)`.
Registry descriptor checks use bounded parallelism.
If one of several registries fails, the job is `Failed` while already verified output digests remain in status.
Registry pushes are not transactional and Kova does not roll them back.

A caller can retry safely by creating a new request with the same immutable source URI, source digest, targets, and build options.
Kova does not resume a failed build internally.
Workloads above 100 logical targets must be split by the caller into several bounded builds.

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
