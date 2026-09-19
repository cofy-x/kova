<p align="center">
  <img src="./assets/readme/hero.svg" width="100%" alt="Kova — a Kubernetes-native image build service: the CLI submits jobs to the controller's KovaBuild API, a per-job runner drives shared rootless BuildKit workers, and images are pushed to an OCI registry and preheated over Dragonfly P2P.">
</p>

<p align="center">
  <a href="https://github.com/cofy-x/kova/actions/workflows/ci.yml"><img src="https://github.com/cofy-x/kova/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
</p>

<p align="center">
  English | <a href="README.zh-CN.md">中文</a>
</p>

Kova is a cloud-neutral seed image build execution plane for agentic infrastructure.
It verifies immutable source bundles, schedules BuildKit, pushes OCI or Nydus images, and returns verified OCI manifest digests.

## Quick start

You need a Kubernetes cluster, Helm with OCI support, and `kubectl`.
Choose a tag from [GitHub releases](https://github.com/cofy-x/kova/releases) so the chart, CLI, and runtime images stay aligned.
Provenance-attested CLI archives for Linux, macOS, and Windows are published there as well.

Install the CLI:

```bash
go install github.com/cofy-x/kova/cmd/kova@latest
kova version
```

Install the service.
The quick-start profile uses a generated static token; shared environments should use TokenReview:

```bash
export KOVA_VERSION=vX.Y.Z
export KOVA_SERVICE_TOKEN=$(openssl rand -hex 32)

kubectl create namespace kova --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kova create secret generic kova-service-auth \
  --from-literal=token="${KOVA_SERVICE_TOKEN}"

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --set serviceDaemon.enabled=true \
  --set serviceDaemon.authentication.mode=static \
  --set serviceDaemon.authentication.staticPrincipal=kova:quickstart \
  --set serviceDaemon.authentication.staticTokenSecret.name=kova-service-auth \
  --wait

kubectl -n kova create rolebinding kova-quickstart \
  --role=kova-service-submitter \
  --user=kova:quickstart
```

Applying the release CRD before every Helm upgrade is required because Helm does not upgrade files from a chart's `crds/` directory.

Run your first build:

```bash
kubectl -n kova port-forward service/kova-service 8080:8080 &

kova ctx set --mode service --service-url http://127.0.0.1:8080 --use quickstart
kova doctor
kova job submit \
  --source-repository registry.example.com/team/kova-sources:quickstart \
  --target registry.example.com/team/image:dev \
  ./image
kova job wait <job-id>
kova job results <job-id>
```

Registry credentials, Nydus output, and batch archives are covered in the [installation and first-build guide](docs/quickstart.md).

## Python SDK

`kova-client` is the official Python-first Service SDK:

```bash
python -m pip install kova-client
```

```python
from kova_client import ClientConfig, CreateBuildRequest, KovaClient

with KovaClient(ClientConfig.from_env()) as kova:
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
    if terminal.status == "succeeded":
        for output in kova.get_results(job.id).outputs:
            print(output.immutable_ref, output.manifest_digest)
```

The synchronous and asynchronous clients expose the same thin HTTP v1 operations and never own workflow recovery or durable receipts.
See the [Python client contract and caller-owned receipt example](docs/service.md#python-sdk).

## Go SDK

External Go callers can use the stable Service API types in `pkg/api/v1` and the shared client used by the Kova CLI in `pkg/client`:

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

Compatibility checks are explicit, so `CreateBuild` performs exactly one submission request.
The SDKs do not own retry or recovery workflows and never automatically retry `CreateBuild` or other mutating requests.
Callers use an idempotency key for safe submission retries and persist the immutable source identity, build ID, manifest digest, and immutable reference before the terminal Kova job expires.
The verified manifest digest and `immutable_ref` are success facts; tags and ephemeral logs are not.
See the [external Go client and HTTP contract](docs/service.md#go-sdk) and the [machine-readable Service API](api/openapi.yaml).

## Why Kova

- **Immutable source to verified image** — every build consumes a digest-verified OCI or HTTPS source and returns the pushed image manifest digest.
- **A bounded execution model** — one immutable `KovaBuild` accepts up to 100 logical targets and records at most 200 concrete outputs; callers own larger workflow partitioning and retries.
- **Fair, work-conserving scheduling** — queued jobs interleave by authenticated requester; admission reserves actual BuildKit worker slots.
- **Isolated execution** — one runner Pod per job drives shared upstream rootless BuildKit workers; controller and runner run as non-root with all capabilities dropped.
- **Kubernetes-native auth** — TokenReview and SubjectAccessReview by default; submitters never touch Pods, Secrets, or other users' jobs.
- **Cloud-provider-neutral** — registry and API credentials are external Secret inputs. The chart creates no clusters, cloud accounts, object stores, or registries.
- **Observable** — stable OpenTelemetry metrics for queue delay, job duration, and capacity waits.

## Documentation

- [Documentation map](docs/README.md): choose the guide for a task.
- [Installation and first build](docs/quickstart.md): OCI chart, matching CLI, and a verified build.
- [Service job workflow](docs/service.md): immutable sources, identity, RBAC, bounded results, and job operations.
- [CLI workflow](docs/cli-workflow.md): direct runner builds for development and low-level debugging.
- [Runtime design](docs/architecture.md): roles, topology, build/export, preheat, and scaling flows.
- [Kubernetes deployment](docs/deployment/kubernetes.md): registry credentials, worker sizing, and production configuration.
- [Release process](docs/releases.md): CLI archives, OCI charts, runtime images, SBOMs, and provenance.
- [Examples](examples/README.md): build input examples and runtime smoke services.

## Develop

The repository requires the Go version declared in `go.mod`, Docker, kind, Helm, kubectl, curl, zip, and LMDB development headers.

```bash
make test
make lint-scripts
make helm-template
make e2e-helm-quickstart   # released-chart install path on kind
```

Use the [validation matrix](docs/testing.md) to choose broader E2E coverage.
Contributions are welcome; the [contribution workflow](CONTRIBUTING.md) covers the full setup and pull request process.
Report vulnerabilities through the private process in the [security policy](SECURITY.md).

## License

Kova is licensed under the [Apache License 2.0](LICENSE).
