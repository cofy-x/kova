# AGENTS.md

This file gives coding agents the minimum context needed to start safely. Read
the [agent context map](.x/README.md) before making changes.

## Project

Kova is a Kubernetes-native, cloud-provider-neutral image build service powered
by BuildKit. It builds batches of Dockerfile contexts into OCI or Nydus images,
pushes them to OCI registries, and can preheat successful build results through
a Dragonfly P2P cluster.

## Hard Rules

- Keep Kova cloud-provider-neutral. Provider accounts, cluster lifecycle,
  credentials, environment overlays, and deployment orchestration belong to
  the consuming environment repository.
- Never commit `.env`, kubeconfig, registry passwords, tokens, or cloud
  credentials.
- Keep behavior stable unless the user explicitly asks for behavior changes.
- Preserve the CLI surface and daemon API paths unless a migration is planned.
- Be careful with dirty worktrees. Do not revert user changes unless asked.
- Use the narrowest useful verification for the change.

## Task Guidance

- [Development conventions](.x/development.md).
- [Validation guide](.x/validation.md).
- [Runtime infrastructure rules](.x/runtime-infrastructure.md).
- [Kubernetes deployment boundaries](.x/deployment.md).
