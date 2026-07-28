# Kova Service

`kova-controller service` is the long-running HTTP gateway and `KovaBuild`
controller.
It authenticates callers, stores immutable source artifacts, admits jobs by
capacity, and creates one short-lived runner Pod per build.

## Authentication

Every `/v1/*` request requires an explicit authentication mode:

- `tokenreview` is the chart default. Bearer tokens are validated with the
  Kubernetes TokenReview API.
- `static` compares a bearer token from `KOVA_SERVICE_AUTH_TOKEN` in constant
  time. Supply the value through a Secret.
- `unsafe-none` disables authentication explicitly and is suitable only for
  an isolated development cluster.

`/healthz` is an unauthenticated process liveness endpoint. `/readyz` verifies
that the controller can query `KovaBuild` resources before it accepts traffic.

## Artifact Storage

The service validates each source archive and computes its SHA-256 digest
before creating a job. The resulting `KovaBuild.spec` is immutable.
Multipart build requests are limited to 1 GiB by default. Set
`serviceDaemon.maxUploadBytes` or `--max-upload-bytes` to change the hard
limit.

Filesystem mode uses a PVC-mounted root:

```bash
kova-controller service \
  --namespace=kova \
  --runner-image=registry.example/kova:runner-v0.1.0 \
  --buildkit-addr=tcp://kova.kova.svc:9094 \
  --artifact-driver=filesystem \
  --artifact-root=/var/lib/kova/artifacts \
  --source-pvc-claim=kova-artifacts
```

S3 mode accepts any S3-compatible endpoint:

```bash
kova-controller service \
  --namespace=kova \
  --runner-image=registry.example/kova:runner-v0.1.0 \
  --buildkit-addr=tcp://kova.kova.svc:9094 \
  --artifact-driver=s3 \
  --artifact-secret=kova-artifact-credentials \
  --s3-endpoint=objects.example.com \
  --s3-bucket=kova-builds \
  --s3-region=us-east-1
```

The referenced Secret exposes `KOVA_S3_ACCESS_KEY`, `KOVA_S3_SECRET_KEY`, and
optional `KOVA_S3_SESSION_TOKEN`. An S3 runner init container uses the same
Secret to download and verify the job source. S3 mode does not require an RWX
volume or controller/runner node affinity.

## API

Create a build:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -F file=@.work/source.zip \
  -F formats=oci,nydus \
  -F idempotency_key=<stable-request-key> \
  -F target=registry.example.com/kova/demo \
  -F concurrency=2 \
  "$BASE/v1/builds"
```

`source_digest` is optional. When supplied, it must match the digest computed
from the uploaded bytes. Responses always contain the computed digest.

The first request returns `202 Accepted`. Repeating an idempotency key with the
same archive and build options returns the existing job with `200 OK`. Reusing
it with different immutable inputs returns `409 Conflict`.

Supported form fields are:

- `file`: required source zip
- `formats`: `oci,nydus` in either order
- `format`: `oci`, `nydus`, or `both` when `formats` is omitted
- `source_digest`: optional lowercase SHA-256 assertion
- `idempotency_key`: optional stable request key, up to 256 characters
- `target`: required output image reference matching archive metadata
- `concurrency`, `timeout`, `retry`, and `oom-cooldown`
- `fail-fast`, `skip-fail`, and `verbose`
- repeated `var` values in `NAME=value` form

Query and control jobs:

```bash
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/results"
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/logs?tail_lines=100"
curl -sS -X POST -H "Authorization: Bearer $TOKEN" "$BASE/v1/builds/<id>/cancel"
```

Status includes Kubernetes Conditions, the observed generation, allocated
concurrency, a typed result summary, and at most 100 inline results. The full
result set is persisted as a JSON artifact and referenced by
`status.resultArtifactURI`.

Export and preheat operate through the typed runner daemon transport:

```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/builds/<id>/export?oci=true"
curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/builds/<id>/preheat?dragonfly-scheduler-addr=dragonfly:8002"
```

## Helm

Enable the service with TokenReview and S3 storage:

```yaml
serviceDaemon:
  enabled: true
  authentication:
    mode: tokenreview
  maxActiveJobs: 20
  workerSlots: 40

artifactStore:
  driver: s3
  secretName: kova-artifact-credentials
  s3:
    endpoint: objects.example.com
    bucket: kova-builds
    region: us-east-1
    secure: true
```

Static-token environments use an externally managed Secret:

```yaml
serviceDaemon:
  authentication:
    mode: static
    staticTokenSecret:
      name: kova-service-auth
      key: token
```

Filesystem mode is useful for local clusters:

```yaml
artifactStore:
  driver: filesystem
  filesystem:
    pvc:
      create: true
      accessModes:
        - ReadWriteOnce
```

A ReadWriteOnce filesystem may require identical controller and runner node
selectors. Horizontally scalable deployments should use S3-compatible storage
instead of introducing an RWX dependency.

The chart creates the service ServiceAccount and RBAC. TokenReview mode also
creates the narrow ClusterRole needed to create TokenReview resources.
Registry, artifact, and static API credentials remain external Secret inputs.
