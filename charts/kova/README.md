# Kova Helm Chart

This chart installs Kova's rootless BuildKit workers and optional authenticated
controller. Published chart versions automatically select controller, runner,
and worker images from the same Kova release.

## Install

```bash
# Replace vX.Y.Z with a tag from https://github.com/cofy-x/kova/releases.
export KOVA_VERSION=vX.Y.Z

helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --wait
```

The default installs one worker. Production environments should provide an
environment-owned values file for capacity, storage, registry credentials,
scheduling, and service configuration.

See the [Quick Start](../../docs/quickstart.md) and
[Kubernetes deployment guide](../../docs/deployment/kubernetes.md) for the
supported workflows and configuration boundaries.
