# Runtime Infrastructure Rules

- `scripts/runtime/dragonfly-nydus-install.sh` owns local kind worker
  containerd mutation for Dragonfly/Nydus validation.
- Verified Helm chart versions are pinned by default:
  `dragonfly/dragonfly` `1.6.27` and `dragonfly/nydus-snapshotter` `0.0.10`.
- The local registry uses `localhost:5002` from the host and
  `host.docker.internal:5002` from Pods/build outputs.
- If a local HTTP proxy is detected on port `7890`, scripts use
  `host.docker.internal:7890` and keep registry and cluster-local names in
  `NO_PROXY`.
- `make deploy-kind` starts the local Compose Grafana/LGTM test stack; see the
  [observability guide](../docs/observability.md).
- Cloud node/containerd mutation is owned by the environment orchestrator.
  Kova keeps only the provider-neutral Nydus and Dragonfly integration contract
  and the local Kind validation path.
