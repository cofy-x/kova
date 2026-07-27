# Contributing to Kova

Thank you for helping improve Kova.

## Before You Start

Use GitHub issues for confirmed bugs and focused feature proposals. For security vulnerabilities, follow the private process in the [security policy](SECURITY.md) instead of opening a public issue.

Kova requires Go, Docker, Helm, kubectl, kind, curl, zip, and LMDB development headers. The exact Go version is declared in `go.mod`.

## Development Workflow

1. Fork the repository and create a focused branch from `main`.
2. Make the smallest coherent change and add or update tests.
3. Run the relevant local checks.
4. Open a pull request that explains the problem, the approach, and the validation performed.

Start with the fast checks:

```bash
make test
make lint-scripts
make helm-template
```

Changes to Kubernetes behavior should also run the relevant E2E target described in the [validation matrix](docs/testing.md). Documentation changes should keep links relative and valid.

## Pull Requests

Keep pull requests reviewable and avoid unrelated refactors. Preserve the CLI and daemon API unless the change includes a migration plan. New deployment behavior must remain cloud-provider-neutral; provider accounts, credentials, cluster lifecycle, and environment overlays belong outside Kova.

By submitting a contribution, you agree that it is licensed under the [Apache License 2.0](LICENSE).
