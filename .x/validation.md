# Validation Rules

Run the narrowest useful verification for the change.

For broad code changes, prefer:

```bash
go test ./...
make lint-scripts
make helm-template
git diff --check
```

For build, registry, Dragonfly, Nydus, or runtime changes, run:

```bash
make e2e-runtime
```

For OTel or logging changes, run:

```bash
make e2e-observability
```

`make e2e-runtime` uses `examples/service-oci` and `examples/service-nydus`;
see the [example catalog](../examples/README.md).

See the [complete validation matrix](../docs/testing.md) for the full E2E
coverage.
