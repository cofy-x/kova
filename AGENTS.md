# AGENTS.md

This file gives coding agents the minimum context needed to start safely. Read
the [agent context map](.x/README.md) before making changes.

## Project

Kova is the Kubernetes-native, cloud-provider-neutral seed image build execution plane for agentic infrastructure.
It verifies immutable source contracts, schedules BuildKit, pushes OCI or Nydus images, and delivers verified OCI manifest digests.
Callers own workflow recovery, retries, long-term logs, and source or image retention.

## Hard Rules

- Keep Kova cloud-provider-neutral. Provider accounts, cluster lifecycle,
  credentials, environment overlays, and deployment orchestration belong to
  the consuming environment repository.
- Never commit `.env`, kubeconfig, registry passwords, tokens, or cloud
  credentials.
- Keep the execution contract bounded: at most 100 logical targets and 200 concrete outputs per build.
- Do not add workflow recovery, durable logs, or a Kova-owned source/result storage lifecycle.
- Breaking changes are acceptable when they remove an invalid boundary; do not preserve compatibility layers without an explicit requirement.
- Be careful with dirty worktrees. Do not revert user changes unless asked.
- Use the narrowest useful verification for the change.

## Task Guidance

- [Development conventions](.x/development.md).
- [Validation guide](.x/validation.md).
- [Runtime infrastructure rules](.x/runtime-infrastructure.md).
- [Kubernetes deployment boundaries](.x/deployment.md).
