# Local Kind Registry

The local registry used by the kind development environment is a Docker
container, not a Kubernetes Pod or Service.

## Addresses

- Host access: `localhost:5002`
- Docker container name: `kind-registry`
- Registry port inside the Docker container: `5000`
- kind node mirror endpoint: `http://kind-registry:5000`
- BuildKit push target from Pods: `host.docker.internal:5002`

## Flow

```mermaid
flowchart LR
  host["Host macOS<br/>Docker client"]
  hostPort["Host port<br/>localhost:5002"]
  registry["Docker container<br/>kind-registry<br/>registry:2 on :5000"]
  kindNode["kind nodes<br/>containerd"]
  kubelet["kubelet / Pod image pull"]
  buildkitSvc["headless Service<br/>kova:9094"]
  buildkit["3 buildkitd Pods<br/>spread across worker nodes"]
  runner["runner Pod<br/>kovad daemon"]

  host -->|"docker push localhost:5002/..."| hostPort
  hostPort -->|"port maps to :5000"| registry

  kindNode -->|"containerd mirror<br/>localhost:5002 -> kind-registry:5000"| registry
  kubelet -->|"image: localhost:5002/kova:dev"| kindNode

  runner -->|"tcp://kova...:9094"| buildkitSvc
  buildkitSvc --> buildkit
  buildkit -->|"push target<br/>host.docker.internal:5002/..."| hostPort
```

## Why `localhost:5002` Works for Pod Images

`deploy/kind-cluster.yaml` configures kind node containerd with:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:5002"]
  endpoint = ["http://kind-registry:5000"]
```

This is only for image pulls performed by kind nodes. When Kubernetes sees an
image such as `localhost:5002/kova:dev`, containerd inside the kind
node uses the mirror endpoint and pulls from the Docker container named
`kind-registry`.

Without this mirror, `localhost:5002` would mean localhost inside the kind node,
not the host machine.

## Why BuildKit Uses `host.docker.internal:5002`

The example build runs inside Pods and pushes the output image from BuildKit.
For that push path, the target registry is passed as:

```text
KOVA_IMAGE_REGISTRY=host.docker.internal:5002
```

This lets the Pod reach the host-exposed registry port. The kind values also
configure BuildKit to treat `host.docker.internal:5002` as an HTTP registry for
local development.

## Related Targets

- `make kind-registry`: starts or reuses the `kind-registry` Docker container.
- `make kind-create`: creates the kind cluster and connects `kind-registry` to
  the kind Docker network.
- `make kind-load`: pushes `localhost:5002/kova:dev` to the local
  registry and loads it into kind nodes.
- `make e2e`: deploys BuildKit workers, creates a runner, builds the example
  image, pushes it, exports results, and verifies host-side pull.
- `make e2e-concurrent`: generates multiple tiny image contexts, builds them
  concurrently through the same headless Service, exports results, and verifies
  every pushed image can be pulled from the host. It also validates the exported
  JSONL result count, success flags, and minimum distinct BuildKit worker IPs.
