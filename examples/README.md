# Examples

This directory contains build inputs used by local E2E and smoke tests. Each
example directory is a build context and includes:

- `Dockerfile`
- `metadata.json` with a `target` field

For one-off local builds, pass a directory directly to `kova build`. For batch
or CI builds, the package scripts zip selected example directories and stream
the archive to `kova build`.

## Image Registry Variable

Examples use `$KOVA_IMAGE_REGISTRY` in `metadata.json` and sometimes in the
Dockerfile. Local E2E scripts set it to `kind-registry:5000`, the stable name
on the Docker `kind` network. Host-side verification pulls the same content
through the published port at `localhost:5002`.

## Example Matrix

| Example | Format | Used By | Purpose |
| --- | --- | --- | --- |
| `simple` | OCI | `make e2e` | Minimal scratch image for the standard build, push, export, and host pull path. |
| `nydus-smoke` | Nydus | `make e2e-dragonfly-nydus` | Minimal Nydus image used to validate convert, export, preheat, and Pod startup. |
| `service-oci` | OCI | `make e2e-runtime` | Python HTTP service used to validate that an OCI image can run behind a Kubernetes Service. |
| `service-nydus` | Nydus | `make e2e-runtime` | Python HTTP service used to validate that a Nydus image can run behind a Kubernetes Service. |

## Runtime Service Smoke

`service-oci` and `service-nydus` intentionally use a local Python smoke base
image:

```dockerfile
FROM $KOVA_IMAGE_REGISTRY/kova-examples/python-smoke-base:dev
```

`make e2e-runtime` builds and pushes that base image before building the
examples. This keeps the runtime smoke self-contained without adding Python to
the production Kova runtime image.

The Python service listens on port `8080` and returns JSON from `/healthz`:

```json
{"service":"kova-runtime-smoke","format":"oci","path":"/healthz","ok":true}
```

For the Nydus service, the `format` value is `nydus`.

`make e2e-runtime` builds both service images, preheats both result modes
through Dragonfly, deploys both images as Kubernetes Deployments and Services,
and validates each Service with an in-cluster curl probe.

## Packaging Selected Examples

Package one or more examples with `EXAMPLE_DIRS`:

```bash
EXAMPLE_DIRS="simple service-oci" ./scripts/package/package-example.sh
```

The generated archive is written to `source.zip` by default. Override
`SOURCE_ZIP` when a test needs an isolated artifact name.
