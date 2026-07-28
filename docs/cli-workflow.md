# CLI Workflow

This guide shows the complete operator flow for one Kova build batch:

```text
prepare -> build -> logs -> wait -> export -> destroy
```

The same CLI flow works against a local kind cluster or any Kubernetes cluster
accepted by the supplied kubeconfig. The runner image must
already be pullable from the target cluster.

## Local kind Setup

Create the local cluster, build the role images, and deploy the shared
BuildKit workers:

```bash
make kind-create
make image
make deploy-kind
make install
```

The local runner image is usually:

```text
localhost:5002/kova:runner-dev
```

`make install` installs the local CLI as `kova` into your Go bin directory. Use
`bin/kova` instead if you prefer the repository-local binary from `make kova`.

Pods use `kind-registry:5000` for build output targets inside the Docker `kind`
network. The host reaches the same registry at `localhost:5002`.

## Configure Contexts

Kova contexts store local defaults for kubeconfig, namespace, BuildKit address,
and runner image. They are stored under your user config directory, usually
`~/.config/kova/config.json`.

Create a local kind context:

```bash
kova ctx set \
  --kubeconfig .kind/kova-local.kubeconfig \
  --namespace default \
  --buildkit-addr tcp://kova.kova.svc:9094 \
  --image localhost:5002/kova:runner-dev \
  --image-pull-policy IfNotPresent \
  --image-pull-secret "" \
  --use \
  kind
```

Create another context for a remote Kubernetes cluster:

```bash
kova ctx set \
  --kubeconfig /path/to/remote.kubeconfig \
  --namespace default \
  --buildkit-addr tcp://kova.kova.svc:9094 \
  --image <registry>/<repo>/kova:runner-<tag> \
  --image-pull-policy IfNotPresent \
  --image-pull-secret kova-registry \
  remote
```

Switch or inspect contexts:

```bash
kova ctx list
kova ctx use kind
kova ctx current
kova ctx show remote
```

You can also select a context for one command:

```bash
kova --ctx remote list
```

## Prepare a Runner

Create one runner Pod for the batch. This command runs from your workstation
and uses the kubeconfig to create the Pod in the target cluster:

```bash
kova --name quickstart prepare
```

The runner Pod starts `kovad daemon` from the runner image.

## Start Builds

For a single image, pass the build context directory directly. If the directory
does not have `metadata.json`, provide the image name with `--target`:

```bash
kova --name quickstart \
  build examples/simple \
  --target kind-registry:5000/kova-examples/simple:dev \
  --format oci --concurrency 1 --timeout 600 --fail-fast --verbose
```

If the directory already has `metadata.json`, you can use its `target` field and
only provide variable values:

```bash
kova --name quickstart \
  build examples/simple \
  --format oci --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var KOVA_IMAGE_REGISTRY=kind-registry:5000
```

Use `--format both` when the same context should produce both the OCI target and
the Nydus target in one build pass. The Nydus target uses the `_nydus_v3` suffix:

```bash
kova --name quickstart \
  build examples/simple \
  --target kind-registry:5000/kova-examples/simple:dev \
  --format both
```

For batch or CI input, Kova also reads a zip stream from stdin. Each image
directory in the zip must contain a `Dockerfile` and `metadata.json`. The
built-in example target uses `$KOVA_IMAGE_REGISTRY`, supplied during `build`:

```bash
mkdir -p .work
cd examples && zip -qr ../.work/source.zip simple && cd ..

kova --name quickstart \
  build --format oci --concurrency 1 --timeout 600 --fail-fast --verbose \
  --var KOVA_IMAGE_REGISTRY=kind-registry:5000 \
  < .work/source.zip
```

The runner daemon unpacks the zip, resolves the `kova` headless Service to
worker Pod IPs, and starts `buildctl` subprocesses that connect to BuildKit
workers.

## Check Logs

Inspect recent runner logs:

```bash
kova --name quickstart logs --tail 100
```

`logs` fetches the latest lines from the runner Pod.

## Wait and Export

Wait for the build to finish:

```bash
kova --name quickstart wait --timeout 600
```

Export successful OCI results to JSONL:

```bash
kova --name quickstart export --result .work/result.jsonl --oci
```

Runner result stores live for the runner Pod lifetime, so an unfiltered export
contains every matching result accumulated by that runner. Build wrappers
should pass repeatable exact targets when the output must describe one batch:

```bash
kova --name quickstart export \
  --result .work/current-batch.jsonl \
  --oci \
  --target registry.example.com/team/app:dev \
  --target registry.example.com/team/base:dev
```

An explicitly requested target that is missing, failed, or excluded by the
selected OCI/Nydus mode makes export fail instead of silently producing a
partial result.

Verify the example image from the host:

```bash
docker pull localhost:5002/kova-examples/simple:dev
```

## Clean Up

Delete the runner Pod when the batch is complete:

```bash
kova --name quickstart destroy
```

Use `make clean-kind` when you want to remove the local cluster.
