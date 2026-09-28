# Observability

Kova can export traces, metrics, and logs through OpenTelemetry. The Go code is
no-op by default. Local Kind runners send telemetry to the host Compose stack;
other Kubernetes environments use the independent observability release.

## Local Compose

```bash
make observability-up
make observability-status
make e2e-observability
```

`make observability-up` starts `grafana/otel-lgtm:0.11.16` through
`deploy/local/compose-observability.yaml`. LGTM is intentionally limited to
local functional validation. Its Kova-specific host ports are OTLP gRPC
`14317`, OTLP HTTP `14318`, Prometheus `19090`, and Grafana `30301`, so it can
run alongside other local observability stacks. Override them with
`KOVA_LOCAL_OTLP_GRPC_PORT`, `KOVA_LOCAL_OTLP_HTTP_PORT`,
`KOVA_LOCAL_PROMETHEUS_PORT`, and `KOVA_LOCAL_GRAFANA_PORT` when necessary.

Use `make observability-status` to verify the local Grafana/LGTM UI at
`http://localhost:30301`, and `make observability-down` to remove the local
stack after validation.

## Kubernetes

Kubernetes environments use the independent `charts/kova-observability`
release. It deploys two OTel Collector replicas with persistent queues plus
separate Prometheus, Tempo, Loki, and Grafana services with PVCs. Do not deploy
the local LGTM image into Kubernetes.

The observability release is independent from the Kova worker release. In a
Kubernetes environment, schedule it on a suitable infrastructure pool and
configure runners with
`kova-observability-collector.kova-observability.svc:4317`.

## Configuration

Kova processes use these environment variables:

- `KOVA_OTEL_ENABLED`
- `KOVA_OTEL_TRACES_ENABLED`
- `KOVA_OTEL_METRICS_ENABLED`
- `KOVA_OTEL_LOGS_ENABLED`
- `KOVA_OTEL_METRIC_INTERVAL`
- `OTEL_SERVICE_NAME`
- `OTEL_SERVICE_VERSION`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPORTER_OTLP_INSECURE`
- `OTEL_RESOURCE_ATTRIBUTES`

Runner creation also supports `KOVA_DAEMON_OTEL_*` variables. These are mapped
into the runner Pod without enabling telemetry in the local `kova` CLI, which
keeps local commands from trying to dial cluster DNS names.

## Helm

The Kova chart exposes an `observability` values block for controller and
runner processes but does not own the telemetry backend. Local Kind values
point those processes at the host Compose stack; the production Kubernetes
baseline keeps telemetry disabled unless a collector endpoint is configured
explicitly. Rootless BuildKit workers are upstream processes and are not
configured with Kova telemetry variables.

## Signals

Kova emits operation spans and metrics for runner actions, daemon HTTP requests,
batch build/export/preheat operations, per-target build/preheat attempts, and
selected Kubernetes client calls. Service metrics also cover queue latency,
capacity waits, terminal outcomes, authentication denials, authorization
denials, and cancellations. Logs keep the existing stderr format
and are also exported as OTel log records when telemetry is enabled.

### Service latency attribution

`kova.service.job.queue.duration` and public `created_at` → `started_at`
measure queue/controller work through runner Pod creation, **not** Pod readiness
or source preparation. `started_at` → `finished_at` includes startup, source,
build execution, polling and receipt verification; it is not BuildKit time.

The following bounded-label diagnostics separate these costs:

- `kova.service.stage.duration`, with `kova.stage` = `admission`,
  `source_inspect` or `submit`, and `kova.result` = `ok` or `error`.
  These measure operation **attempts**, including API client throttling and CAS
  wait. Admission returning a capacity wait is a successful attempt, not a grant.
- `kova.service.runner.startup.duration`, with `kova.stage` = `queue_to_pod`,
  `pod_schedule`, `source_wait`, `source_fetch`, `runner_ready` or `pod_startup`.
  These use CR/Pod creation, Scheduled/Ready conditions and successful
  `source-fetch` init-container timestamps. `source_wait` includes image pull
  and sandbox setup; `source_fetch` is init-container wall time, not just network
  transfer. `pod_startup` overlaps the fine segments: do not add it to them.
- Existing runner `kova_build_target_duration_seconds` and exported result
  `started_at`/`finished_at` measure target execution (BuildKit plus image export
  and push for OCI). Nydus additionally includes conversion and push.
- `kova.service.target.execution.duration` copies those successful target
  receipt timestamps into the controller, labeled only by `kova.mode` = `oci`
  or `nydus`. `Target execution timing` logs remain observable after runner
  deletion. Malformed/backwards receipt timestamps omit the observation without
  changing output-verification correctness. Retries after an uncertain status
  write may duplicate an observation; failed targets are not success samples.

Structured controller logs `Service stage timing` and `Runner startup timing`
carry milliseconds for environments without an OTel backend. Build identity
appears only in logs, never in the metric labels. Startup observations are emitted
after the Starting transition persists; telemetry is best effort, not an
exactly-once receipt. Missing/backwards timestamps are omitted, not zero-filled.
Compare client p95/p99, queue, these segments, target execution, API/CAS errors,
and node **requests as well as usage** before increasing concurrent admission.
