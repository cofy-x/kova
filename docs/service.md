# Kova Service Daemon

`kovad service` runs a long-lived HTTP gateway inside Kubernetes. It accepts
build requests, creates one short-lived runner Pod per build job, and lets that
runner execute `kovad daemon` operations over `/tmp/kova.sock`.
Build state is stored in `KovaBuild` custom resources, and uploaded source
archives are stored on a short-lived PVC path.

Kova supports two client paths: the HTTP service API for platform integration,
and the CLI for direct control of a selected runner Pod through `kova prepare`,
`kova build`, `kova export`, and `kova preheat`.

## Start

Inside a cluster, the service uses the Pod service account through Kubernetes
in-cluster config:

```bash
kovad service \
  --listen=:8080 \
  --namespace=default \
  --runner-image=localhost:5002/kova:dev \
  --runner-image-pull-policy=IfNotPresent \
  --runner-image-pull-secret=kova-registry \
  --buildkit-addr=tcp://kova.kova.svc:9094 \
  --source-pvc-claim=kova-sources \
  --job-ttl=2h
```

If `--auth-token` or `KOVA_SERVICE_AUTH_TOKEN` is set, all `/v1/*` requests must include:

```text
Authorization: Bearer <token>
```

`/healthz` does not require authentication.

## API

Create a build:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -F file=@.work/source.zip \
  -F formats=oci,nydus \
  -F source_digest=sha256:<canonical-source-digest> \
  -F idempotency_key=<stable-publish-key> \
  -F target=registry.example.com/kova/examples/demo \
  -F concurrency=2 \
  "$BASE/v1/builds"
```

The first response is `202 Accepted`. Repeating the same idempotency key with
the same source digest, target, and formats returns the existing job with
`200 OK`; reusing the key with different immutable parameters returns
`409 Conflict`.

```json
{
  "id": "6c8d2a5f4b8e3310",
  "status": "queued",
  "pod_name": "kova-job-6c8d2a5f4b8e3310",
  "namespace": "default",
  "created_at": "2026-06-20T00:00:00Z"
}
```

Supported build form fields are:

- `file`: required source zip.
- `formats`: comma-separated `oci`, `nydus`, or both.
- `format`: optional singular alias used when `formats` is omitted.
- `source_digest`: logical digest produced by the caller's deterministic
  compiler.
- `idempotency_key`: caller-stable key for one source/target/formats request.
- `target`: required destination image reference; it must match the single
  `metadata.json` target in the uploaded archive.
- `concurrency`, `timeout`, `retry`, `oom-cooldown`.
- `fail-fast`, `skip-fail`, `verbose`.
- repeated `var`: build variables in `NAME=value` form.

Query and control jobs:

```bash
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/results"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/logs?tail_lines=100"
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/cancel"
```

`GET /v1/builds/<id>/results` returns typed results for every requested
target/format, including status, repository, registry-resolved manifest digest,
media type, size, and any error. Kova resolves the final registry descriptor
after push; callers must not parse logs or NDJSON to discover a digest. Results,
the source digest, and the idempotency key are persisted in `KovaBuild` status
and exposed in job responses.

The service reserves the `KovaBuild` before accepting the shared-PVC source
archive. The controller starts no runner until `spec.source.ready=true`, which
is written only after the archive is atomically committed and its single-image
target matches the request. This makes idempotency safe across concurrent
service replicas.

If TTL cleanup has removed a job, its idempotency record is gone and the same
logical source may be submitted again. Cancellation and log requests identify
the build by its job id.

Export and preheat use the runner daemon state for that job:

```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/export?oci=true"
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/preheat?dragonfly-scheduler-addr=dragonfly:8002"
```

## Helm

The chart keeps the worker-only deployment by default. Enable the HTTP gateway
explicitly:

```yaml
serviceDaemon:
  enabled: true
  authTokenSecret:
    name: kova-service-auth
    key: token
  runnerImage: ""
  buildkitAddr: ""
sourceStore:
  pvc:
    existingClaim: kova-sources
```

When enabled, the chart renders a separate Deployment, ClusterIP Service,
ServiceAccount, Role, RoleBinding, and the `KovaBuild` CRD. Production
deployments should provide an existing ReadWriteMany PVC backed by a shared
CSI-compatible filesystem.
The service pod mounts the configured registry pull secret as Docker client
credentials so typed result resolution works with private registries; API
bearer tokens are read from `authTokenSecret`, never rendered in pod args.
When `imagePullSecrets.create=false`, that Secret is an external deployment
input: the chart references it but never creates, adopts, or rotates it. The
environment orchestrator must synchronize it before Helm renders workloads so
both the service and its runner Pods can start from a fresh namespace.

Single-node development or regression clusters can use a chart-created
ReadWriteOnce PVC only when both the service and every runner are constrained
to the same node. Apply the same dedicated node label to
`serviceDaemon.nodeSelector` and `serviceDaemon.runnerNodeSelector`:

```yaml
serviceDaemon:
  enabled: true
  nodeSelector:
    kova.example/source-node: "true"
  runnerNodeSelector:
    kova.example/source-node: "true"
sourceStore:
  pvc:
    create: true
    accessModes:
      - ReadWriteOnce
```

This is not a horizontally scalable production configuration: a RWO source
store and pinned node form one failure domain. Production deployments should
use RWX storage and leave both selectors unconstrained unless another explicit
scheduling policy is required.

Runtime process commands live under `kovad`; the `kova` binary is the
workstation and CI client.
