# Agent Context

These notes extend the [repository agent guide](../AGENTS.md). They describe
how to work in Kova; public product and contributor documentation stays in
`docs/`.

## Repository Map

- `cmd/kova`: workstation and CI client entrypoint.
- `cmd/kovad`: runner daemon and service-daemon entrypoint.
- `internal/app`, `internal/runner`, `internal/kube`: client and Kubernetes
  control flow.
- `internal/daemon`, `internal/batch`, `internal/source`, `internal/store`:
  build execution and result state.
- `internal/service`: long-lived HTTP service and `KovaBuild` controller.
- `charts/`: provider-neutral Kova and observability Helm charts.
- `deploy/`: local and non-local Kubernetes values baselines.
- `scripts/`: build, Kind, packaging, runtime, and E2E entrypoints.
- `examples/`: source archives used by examples and E2E tests.
- `docs/`: public architecture, usage, deployment, and validation guides.

## Task Routing

- [Coding conventions](development.md): code, scripts, chart, and API stability
  conventions.
- [Change validation guide](validation.md): which checks to run for different
  change types.
- [Runtime Infrastructure](runtime-infrastructure.md): local registry,
  Dragonfly, Nydus, proxy, and observability infrastructure notes.
- [Deployment boundaries](deployment.md): provider-neutral Kubernetes and
  environment orchestration boundaries.

## Context Path

For most tasks, read in this order:

1. [Repository agent guide](../AGENTS.md) for the quick safety context.
2. [Documentation map](../docs/README.md) to choose the relevant project docs.
3. The focused rule file in this directory when the task touches that area.

## Common Verification

```bash
go test ./...
make docs-check
make lint-scripts
make helm-template
git diff --check
```

Use the [complete validation matrix](../docs/testing.md) when a change touches
build, registry, Dragonfly, Nydus, runtime, or observability behavior.
