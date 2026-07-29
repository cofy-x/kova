# Deployment Rules

The local kind flow is the self-contained development deployment.

For remote Kubernetes environments:

- Publish the controller, runner, and worker images to a registry reachable by
  the target cluster.
- Override image, resources, replicas, registry auth, and scheduling through
  values files.
- Keep Dragonfly/Nydus as cluster infrastructure.
- Keep node/containerd bootstrap outside Kova application code.
- Use `deploy/production-values.yaml` as the non-local values baseline.

Cloud-provider overlays, cluster lifecycle, registry provisioning, credentials,
load-balancer annotations, and environment-specific E2E orchestration are
environment inputs. Keep Kova's image, charts, CLI, and documented
Kubernetes/registry contracts provider-neutral.
