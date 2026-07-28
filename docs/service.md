# Kova Service

`kova-controller service` is the long-running HTTP gateway and `KovaBuild`
controller. It authenticates callers, stores immutable source artifacts,
admits jobs by capacity, and creates one short-lived runner Pod per build.

## Authentication

Every `/v1/*` request requires an explicit authentication mode:

- `tokenreview` is the chart default. Bearer tokens are validated with the
  Kubernetes TokenReview API.
- `static` compares a bearer token from `KOVA_SERVICE_AUTH_TOKEN` in constant
  time. Supply the value through a Secret.
- `unsafe-none` disables authentication explicitly and is suitable only for
  an isolated development cluster.

Authentication returns a principal containing the caller's Kubernetes
username, UID, and groups. TokenReview mode submits a SubjectAccessReview for
the requested `KovaBuild` operation:

- callers with `create` access can submit jobs
- submitters can list, read, cancel, export, and preheat only their own jobs
- callers with the corresponding namespace-wide RBAC verb can operate on all
  jobs

The chart creates unbound submitter and admin Roles. An environment grants
access with a RoleBinding such as:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kova-builders
  namespace: kova
subjects:
  - kind: Group
    name: kova-builders
    apiGroup: rbac.authorization.k8s.io
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: kova-service-submitter
```

Bind trusted platform operators to `kova-service-admin`. Static mode maps its
single token to `serviceDaemon.authentication.staticPrincipal`; create a
RoleBinding for that Kubernetes username. Kova contexts never store bearer
tokens.

`/healthz` is an unauthenticated process liveness endpoint. `/readyz` verifies
that the controller can query `KovaBuild` resources before it accepts traffic.

## CLI

Create a Service context. A kubeconfig supplies TokenReview credentials through
the same bearer token or exec credential plugin used by Kubernetes clients:

```bash
kubectl -n kova port-forward service/kova-service 8080:8080

kova ctx set \
  --mode service \
  --service-url http://127.0.0.1:8080 \
  --kubeconfig "${KUBECONFIG:-$HOME/.kube/config}" \
  --use \
  service
```

For static authentication, provide the token only through the process
environment:

```bash
export KOVA_SERVICE_TOKEN=REPLACE_WITH_TOKEN
```

Submit and manage jobs without constructing HTTP requests:

```bash
kova job submit ./image \
  --target registry.example.com/team/image:dev \
  --format oci \
  --idempotency-key build-123

kova job list
kova job get <job-id>
kova job logs <job-id> --tail 100
kova job wait <job-id> --timeout 10m
kova job results <job-id>
kova job cancel <job-id>
```

For a batch archive whose top-level image directories already contain
`Dockerfile` and `metadata.json`, omit `--target`. Kova records every immutable
archive target in the job spec and verifies each pushed result:

```bash
kova job submit source.zip --format oci --concurrency 3
```

`--target` remains required when submitting a context directory and, when
provided for a zip, requires exactly one matching archive target.

Use `--service-ca-file` for a private Service CA. The
`--service-insecure` option is intended only for isolated TLS testing.

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
  --runner-image=registry.example/kova:runner-vX.Y.Z \
  --buildkit-addr=tcp://kova.kova.svc:9094 \
  --artifact-driver=filesystem \
  --artifact-root=/var/lib/kova/artifacts \
  --source-pvc-claim=kova-artifacts
```

S3 mode accepts any S3-compatible endpoint:

```bash
kova-controller service \
  --namespace=kova \
  --runner-image=registry.example/kova:runner-vX.Y.Z \
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

## HTTP API

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
same archive and build options returns the existing job with `200 OK`.
Idempotency keys are scoped to the authenticated username. Reusing one with
different immutable inputs returns `409 Conflict`.

Supported form fields are:

- `file`: required source zip
- `formats`: `oci,nydus` in either order
- `format`: `oci`, `nydus`, or `both` when `formats` is omitted
- `source_digest`: optional lowercase SHA-256 assertion
- `idempotency_key`: optional stable request key, up to 256 characters
- `target`: optional single-image override; when omitted, all archive metadata
  targets are built
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

The submitter and admin Roles are intentionally not bound by the chart. The
consuming environment owns user and group membership.

Static-token environments use an externally managed Secret:

```yaml
serviceDaemon:
  authentication:
    mode: static
    staticPrincipal: kova:ci
    staticTokenSecret:
      name: kova-service-auth
      key: token
```

The controller verifies each pushed image descriptor before marking a job
successful. It reuses `serviceDaemon.runnerImagePullSecret`, then
`imagePullSecrets.name`, unless `serviceDaemon.registrySecret` names a distinct
`kubernetes.io/dockerconfigjson` Secret in the release namespace. Registry
transport is HTTPS by default. Isolated development registries that provide
only HTTP must be listed explicitly:

```yaml
serviceDaemon:
  registryPlainHTTP:
    - registry.local:5000
```

Do not enable plain HTTP for production registries.

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
