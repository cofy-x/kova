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
capacity waits, terminal outcomes, artifact writes, authentication denials,
authorization denials, and cancellations. Logs keep the existing stderr format
and are also exported as OTel log records when telemetry is enabled.
