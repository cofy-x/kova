# Architecture

`kova` is a distributed image build service. It turns batches of
Dockerfile contexts into OCI or Nydus images, pushes them to a target registry,
and can preheat built images into a Dragonfly P2P cluster.

## Runtime Roles

- **kova CLI**: the `kova` binary running on a developer workstation or CI job.
  It uses the Kubernetes API to create runner Pods and send build, export,
  wait, log, exec, scale, and preheat requests.
- **runner**: a short-lived Pod for one build batch. It runs
  `kovad daemon`, owns the batch state, unpacks the source zip, records
  results, and dispatches work to BuildKit workers.
- **worker**: a `buildkitd` Pod deployed by the Helm chart. Workers execute
  BuildKit builds and push OCI image outputs to the configured registry. For
  Nydus targets, the runner converts the OCI output with `nydusify` and pushes
  the Nydus image.
- **store**: per-runner result storage. The runner writes build outcomes to
  `result.lmdb`, writes failure logs to `logs.jsonl`, and exports selected
  records to `.work/result.jsonl` in local E2E flows.
- **service daemon**: optional long-running HTTP gateway deployed in the
  cluster. It accepts `/v1/builds` requests, creates one runner Pod per job, and
  proxies status, logs, export, preheat, and cancel operations to that runner.

Each build batch should usually use an independent runner. Build throughput is
scaled mainly by increasing worker replicas and build concurrency. A single
runner distributes its batch across available worker addresses.

## Runtime Image And Processes

Kova uses one runtime image for both runner and worker Pods. The Pod command
selects the role:

- runner Pods start `kovad daemon --addrs <buildkit-addresses>`
- worker Pods start `buildkitd --config /etc/buildkit/buildkitd.toml --addr
  tcp://0.0.0.0:9094`

The runtime image contains `kovad` and the external tools used by the runtime
roles:

- `kovad`: Kova runtime daemon and service entrypoint
- `buildctl`: the upstream BuildKit client used by the runner daemon
- `buildkitd`: the upstream BuildKit daemon used by worker Pods
- `nydusify` and `nydus-image`: Nydus conversion tools
- `grpcurl`: diagnostics and runtime checks

The local `kova` CLI is a Kubernetes client. During `prepare`, it uses the
configured kubeconfig to create the runner Pod. The runner Pod then starts
`kovad daemon` and listens on `/tmp/kova.sock`. Later local commands use
Kubernetes exec to call the daemon through that Unix socket.

For service-style deployments, `kovad service` runs as a separate long-lived
Deployment. It uses in-cluster Kubernetes credentials to create runner Pods and
then uses the same Unix-socket daemon API inside each runner. This keeps the
batch isolation boundary intact while allowing CI systems and platform services
to call Kova through HTTP.

`prepare` is the boundary between the local CLI and the target Kubernetes
cluster. The CLI can point at a local kind cluster or any other Kubernetes
cluster that accepts the kubeconfig. The runner image passed with
`prepare --image` must already be pullable by that cluster. Kova then creates a
runner Pod from that image and waits for the daemon inside the Pod to become
ready.

Build requests run inside the runner Pod. The daemon unpacks source input,
selects BuildKit worker addresses, and starts `buildctl` subprocesses. Those
`buildctl` subprocesses connect to remote worker `buildkitd` Pods; they are not
the component doing the heavy BuildKit execution themselves.

## Worker Address Discovery

Worker Pods are created by the Helm-managed Kova Deployment. Their count comes
from `deployment.replicas` unless chart autoscaling is enabled. A headless
Service named `kova` selects those worker Pods and exposes port `9094`.

The default runner BuildKit address is:

```text
tcp://kova.kova.svc:9094
```

Inside the runner Pod, Kubernetes DNS resolves the headless Service name to the
ready worker Pod IPs. The scheduler parses the address, resolves the hostname
with DNS, and turns every returned IP into an independent BuildKit endpoint:

```text
tcp://10.244.1.3:9094
tcp://10.244.2.2:9094
tcp://10.244.3.3:9094
```

Kova does not list worker Pods through the Kubernetes API for scheduling. It
relies on headless Service DNS records, then uses its scheduler package to keep
an address pool, assign targets consistently, enforce per-address concurrency,
and temporarily cool down workers after OOM-style connection failures.

## Topology

```mermaid
flowchart LR
  client["kova CLI<br/>workstation"]
  api["Kubernetes API"]
  runnerPod["runner Pod<br/>runtime image"]
  daemon["kovad daemon<br/>/tmp/kova.sock"]
  buildctl["buildctl subprocesses"]
  socket["Unix socket<br/>/tmp/kova.sock"]
  dns["Kubernetes DNS<br/>kova.kova.svc"]
  service["Headless Service<br/>kova:9094"]
  workerA["worker Pod<br/>runtime image<br/>buildkitd"]
  workerB["worker Pod<br/>runtime image<br/>buildkitd"]
  workerC["worker Pod<br/>runtime image<br/>buildkitd"]
  registry["target registry"]
  dragonfly["Dragonfly P2P cluster"]

  client -->|"create/delete/list/logs/exec/scale"| api
  api --> runnerPod
  client -->|"remote exec curl"| socket
  runnerPod --> daemon
  socket --> daemon
  daemon -->|"spawn"| buildctl
  buildctl -->|"resolve"| dns
  dns --> service
  buildctl -->|"buildctl --addr tcp://pod-ip:9094"| service
  service --> workerA
  service --> workerB
  service --> workerC
  workerA -->|"push image"| registry
  workerB -->|"push image"| registry
  workerC -->|"push image"| registry
  daemon -->|"convert and push Nydus targets"| registry
  daemon -->|"preheat selected images"| dragonfly
```

## Service Gateway Topology

```mermaid
flowchart LR
  client["CI / platform / curl"]
  gateway["kovad service<br/>HTTP :8080"]
  api["Kubernetes API"]
  runnerPod["runner Pod per job<br/>kovad daemon"]
  socket["/tmp/kova.sock"]
  worker["BuildKit workers"]
  registry["target registry"]

  client -->|"POST /v1/builds"| gateway
  gateway -->|"create/delete/exec/logs"| api
  api --> runnerPod
  gateway -->|"exec curl --unix-socket"| socket
  runnerPod --> socket
  runnerPod --> worker
  worker --> registry
```

## Build Batch Flow

```mermaid
sequenceDiagram
  autonumber
  participant Client as kova CLI
  participant API as Kubernetes API
  participant Runner as runner Pod
  participant Daemon as kovad daemon
  participant Worker as buildkitd workers
  participant Registry as target registry

  Client->>API: prepare using kubeconfig
  Client->>API: create runner Pod with --image
  API->>Runner: start container: kovad daemon --addrs ...
  Runner->>Daemon: listen on /tmp/kova.sock
  API-->>Client: runner Pod Ready
  Client->>Runner: health check through Kubernetes exec
  Runner->>Daemon: GET /api/v1/health
  Daemon-->>Runner: ok
  Runner-->>Client: ready

  Client->>Runner: POST /api/v1/build with .work/source.zip
  Runner->>Daemon: stream zip over Unix socket
  Daemon->>Daemon: unpack source, parse metadata, apply variables
  Daemon->>Daemon: resolve headless Service DNS into worker IPs
  loop For each image target
    Daemon->>Daemon: select worker address from scheduler pool
    Daemon->>Worker: spawn buildctl --addr tcp://worker-ip:9094 build
    Worker->>Registry: push OCI image
    opt Nydus target
      Daemon->>Registry: nydusify convert and push Nydus image
    end
    Worker-->>Daemon: build result
    Daemon->>Daemon: persist result and failure logs
  end
  Daemon-->>Runner: build accepted/status
  Runner-->>Client: build response
```

## Export Flow

```mermaid
sequenceDiagram
  autonumber
  participant Client as kova CLI
  participant Runner as runner Pod
  participant Daemon as kovad daemon
  participant Store as result store

  Client->>Runner: POST /api/v1/export
  Runner->>Daemon: request export over Unix socket
  Daemon->>Store: read result.lmdb
  Store-->>Daemon: stored build entries
  Daemon->>Daemon: filter OCI / include failures
  Daemon-->>Runner: result JSONL stream
  Runner-->>Client: write .work/result.jsonl
```

## Preheat Flow

```mermaid
sequenceDiagram
  autonumber
  participant Client as kova CLI
  participant Runner as runner Pod
  participant Daemon as kovad daemon
  participant Store as result store
  participant Dragonfly as Dragonfly scheduler

  Client->>Runner: POST /api/v1/preheat
  Runner->>Daemon: request preheat over Unix socket
  Daemon->>Store: read successful build entries
  loop For each selected image
    Daemon->>Dragonfly: submit preheat request
    Dragonfly-->>Daemon: preheat accepted or failed
    Daemon->>Store: update preheat outcome
  end
  Daemon-->>Runner: preheat summary
  Runner-->>Client: preheat response
```

## Scaling Model

```mermaid
flowchart TB
  batch["one build batch"]
  runner["one runner Pod"]
  concurrency["build concurrency"]
  pool["BuildKit address pool"]
  workers["worker replicas"]

  batch --> runner
  runner --> concurrency
  concurrency --> pool
  pool --> workers

  note1["Increase worker replicas for more BuildKit capacity"]
  note2["Increase build concurrency to use more available workers"]
  note3["Use separate runners to isolate separate batches"]

  workers --- note1
  concurrency --- note2
  runner --- note3
```

## Operational Boundaries

- The runner is the batch isolation boundary. It owns one batch's daemon state,
  result database, failure logs, and exported results.
- Workers are shared build capacity. They do not own batch state.
- Registry credentials and target registry addresses are supplied through Helm
  values, runner options, image pull secrets, and build variables.
- Every Kubernetes deployment uses the same runtime flow: client, runner Pod,
  `kova` headless Service, and BuildKit worker Pods.
