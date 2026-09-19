# Kova Helm Chart

This chart installs Kova's rootless BuildKit workers and optional authenticated
controller. Published chart versions automatically select controller, runner,
and worker images from the same Kova release.

## Install

```bash
# Replace vX.Y.Z with a tag from https://github.com/cofy-x/kova/releases.
export KOVA_VERSION=vX.Y.Z

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --set-string worker.platform=linux/amd64 \
  --wait
```

Always apply the selected release CRD before upgrading. Helm installs files in
`crds/` for a new release but does not update them on later upgrades.

Each release is one explicit `worker.platform` pool (`linux/amd64` or `linux/arm64`) and uses the standard Kubernetes OS and architecture labels. A Service release can route both platforms through explicit `serviceDaemon.buildkitPlatformAddrs` mappings to two BuildKit Services. The default installs one `linux/amd64` worker. Production environments should provide an
environment-owned values file for capacity, registry credentials,
scheduling, and service configuration.

`values.schema.json` rejects unknown or invalid chart values. The worker
NetworkPolicy allows Kova runner Pods from the configured runner namespace by
default. Service ingress policy is opt-in; an empty peer list denies ingress
when it is enabled.

Service deployments reuse the runner image pull Secret to authenticate output
descriptor verification by default. Set `serviceDaemon.registrySecret` when
those credentials differ. The same Secret authorizes OCI source pulls.
Plain HTTP registries must be listed explicitly in
`serviceDaemon.registryPlainHTTP` and are intended only for development.

The chart does not provision object storage or a shared PVC.
Runner Pods use job-local `emptyDir` storage for digest-verified source bundles.

See the [Quick Start](../../docs/quickstart.md) and
[Kubernetes deployment guide](../../docs/deployment/kubernetes.md) for the
supported workflows and configuration boundaries.
