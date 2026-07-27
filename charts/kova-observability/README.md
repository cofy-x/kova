# Kova Observability Chart

This chart is the Kubernetes observability data plane for Kova. It is installed
as an independent release and does not own or restart Kova BuildKit workers.

It deploys:

- two OpenTelemetry Collector replicas with persistent exporter queues
- Prometheus with Kubernetes discovery and persistent TSDB storage
- Tempo and Loki with local persistent storage
- Grafana with provisioned Kova dashboards and datasources

Production environments must provide an existing Grafana admin Secret and a
storage class. Schedule the release on system or platform nodes rather than
Kova build workers.

```bash
helm upgrade --install kova-observability charts/kova-observability \
  --namespace kova-observability \
  --create-namespace \
  --set fullnameOverride=kova-observability \
  --set storageClassName=REPLACE_ME \
  --set grafana.admin.existingSecret=grafana-admin
```

Configure Kova runners with:

```text
kova-observability-collector.kova-observability.svc:4317
```

`grafana/otel-lgtm` is not part of this chart. It is reserved for the local
Compose functional test in `deploy/local/compose-observability.yaml`.
