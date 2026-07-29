# Testing

Kova uses layered checks. Run the narrowest check that matches a change, and run
the full runtime smoke after changing build, registry, Dragonfly, Nydus, or
container runtime behavior.

## Static Checks

```bash
go test ./...
make docs-check
make lint-scripts
make helm-template
git diff --check
```

## Network Overrides

Kova defaults to the official Ubuntu image, upstream GitHub release URLs,
`go.dev`, and `proxy.golang.org`. `scripts/build/build-image.sh` forwards the
standard `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` variables and accepts these
optional source overrides:

- `UBUNTU_IMAGE`
- `APT_MIRROR`
- `BUILDKIT_ROOTLESS_IMAGE`
- `GRPCURL_DOWNLOAD_BASE_URL`
- `NYDUS_DOWNLOAD_BASE_URL`
- `GO_DOWNLOAD_BASE_URL`
- `GOPROXY`

These inputs are generic build controls. Keep provider-specific mirror values
and environment policy in the consuming workspace rather than this repository.

## E2E Matrix

| Target | Coverage | Main Artifacts |
| --- | --- | --- |
| `make e2e` | Zip-stream OCI build plus single-directory OCI build, push, export, and host pull. | `examples/simple`, `.work/result.jsonl` |
| `make e2e-helm-quickstart` | Packages the chart, installs it into a minimal kind cluster, and runs the authenticated Service workflow against the packaged chart. | Helm archive, `examples/simple`, `.work/result-service.jsonl` |
| `make e2e-service` | Virtual-resource RBAC isolation, doctor checks, authenticated build, persisted result and log digests, declarative cancellation, TTL cleanup, and host pull. | `examples/simple`, `.work/result-service.jsonl` |
| `make e2e-service-s3` | The Service workflow against a pinned MinIO deployment, including source materialization, persisted results and logs, and object cleanup after TTL. | `examples/simple`, `.work/result-service-s3.jsonl` |
| `make e2e-release KOVA_VERSION=vX.Y.Z` | Downloads and verifies the exact public CLI and OCI chart, pulls matching public role images, then runs the S3-backed Service lifecycle in a clean kind cluster. | GitHub release assets, public OCI artifacts, `.work/result-released-artifact.jsonl` |
| `make e2e-concurrent` | Multi-image OCI build with worker distribution checks. | generated concurrent examples, `.work/result-concurrent.jsonl` |
| `make e2e-dragonfly-nydus` | Nydus conversion, export, Dragonfly preheat, and Pod startup. | `examples/nydus-smoke`, `.work/result-nydus.jsonl` |
| `make e2e-runtime-preflight` | Local tool and registry readiness checks for runtime validation. | Docker, kind, Helm, kubectl, local registry |
| `make e2e-runtime` | OCI and Nydus service images running behind Kubernetes Services with in-cluster HTTP probes. | `examples/service-oci`, `examples/service-nydus`, runtime result JSONL files |
| `make e2e-observability` | Starts local Compose LGTM, runs a build, and checks Prometheus telemetry. | Grafana/LGTM, `examples/simple` |

Tune the concurrent check with `EXAMPLE_COUNT`, `BUILD_CONCURRENCY`, and
`MIN_BUILDKIT_NODE_IPS`.

## Runtime Smoke

`make e2e-runtime` is the strongest local validation target. It:

- verifies local tools and starts the local registry when needed
- builds and publishes the local controller, runner, and worker tags
- builds and publishes `localhost:5002/kova-examples/python-smoke-base:dev`
- deploys Kova workers
- installs Dragonfly and Nydus
- builds OCI and Nydus Python service images
- preheats both result sets through Dragonfly
- deploys both images as Kubernetes Deployments and Services
- validates both Services with in-cluster curl probes

The expected probe responses are:

```json
{"service":"kova-runtime-smoke","format":"oci","path":"/healthz","ok":true}
```

and:

```json
{"service":"kova-runtime-smoke","format":"nydus","path":"/healthz","ok":true}
```

## Generated Files

Runtime and E2E targets generate source archives and result JSONL files in the
repository root. These are ignored by git and can be removed with:

```bash
make clean
```

Use `make clean-kind` when the local kind cluster itself needs to be recreated.
Use `make diagnose-kind` to collect local registry, kind, Kova, Dragonfly/Nydus,
runtime smoke, event, log, and result summaries while debugging failures.
