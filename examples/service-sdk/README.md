# Service SDK examples

These executable Go and Python examples implement the same bounded seed build flow through the public Kova Service API.
They submit one immutable source and one explicit target platform, wait for a terminal state, read every server-verified output, and atomically write a caller-owned receipt.
Kova does not retain this receipt or own the caller's retry and recovery workflow.

## Environment

Both examples use the same required variables:

| Variable | Meaning |
| --- | --- |
| `KOVA_SERVICE_URL` | Absolute Kova Service HTTP(S) URL. |
| `KOVA_SERVICE_TOKEN` | Bearer token; never written to output or the receipt. |
| `KOVA_SOURCE_URI` | Immutable OCI manifest URI or immutable HTTPS archive URL. |
| `KOVA_SOURCE_DIGEST` | SHA-256 digest of the source archive bytes. |
| `KOVA_TARGET` | Explicit tagged OCI push destination. |
| `KOVA_PLATFORM` | `linux/amd64` or `linux/arm64`. |
| `KOVA_IDEMPOTENCY_KEY` | Caller-owned key for safe submission retries. |
| `KOVA_RECEIPT_PATH` | Destination for the caller-owned JSON receipt. |
| `KOVA_RECIPE_DIGEST` | Caller-owned SHA-256 identity of the complete build recipe. |

Optional variables are `KOVA_TARGET_ROLE` (default `seed`), `KOVA_BUILD_FORMAT` (default `oci`), `KOVA_WAIT_TIMEOUT_SECONDS` (default `600`), `KOVA_POLL_INTERVAL_SECONDS` (default `2`), `KOVA_SERVICE_CA_FILE`, and `KOVA_SERVICE_INSECURE`.

```bash
export KOVA_SERVICE_URL=https://kova.example.com
export KOVA_SERVICE_TOKEN=REPLACE_WITH_TOKEN
export KOVA_SOURCE_URI='oci://registry.example.com/team/sources@sha256:<manifest-digest>'
export KOVA_SOURCE_DIGEST='sha256:<source-content-digest>'
export KOVA_RECIPE_DIGEST='sha256:<recipe-digest>'
export KOVA_TARGET='registry.example.com/team/seed:build-123'
export KOVA_PLATFORM=linux/amd64
export KOVA_IDEMPOTENCY_KEY=build-123
export KOVA_RECEIPT_PATH=./build-123.receipt.json
```

## Go

The Go example imports only the public `pkg/api/v1` and `pkg/client` packages:

```bash
go run ./examples/service-sdk/go
```

External modules should select an exact release from the [Kova release page](https://github.com/cofy-x/kova/releases) and require `github.com/cofy-x/kova@vX.Y.Z`.

## Python

Install an exact released Python package, then run the example:

```bash
python -m pip install 'kova-client==<version>'
python examples/service-sdk/python/main.py
```

## Terminal and receipt semantics

`CreateBuild`/`create_build` is attempted exactly once; neither example implements hidden mutating retries.
The wait is bounded by the configured timeout and can be cancelled with `SIGINT` or `SIGTERM`.
Successful builds print each verified `format`, `manifest_digest`, and server-returned `immutable_ref` and exit zero.
Failed or cancelled builds still request results, atomically persist any already verified outputs with the terminal status and failure code, print no immutable output facts to stdout, and exit with status 3.
Configuration, API, or protocol errors exit with status 1; timeout or caller cancellation exits with status 2.

Outputs are sorted by role, platform, format, image, manifest digest, and immutable reference before persistence, with OCI before Nydus.
The receipt uses `kova.seed-build-receipt/v1`, matching the [reference receipt shape](../../docs/examples/seed-build-receipt-v1.json); CI prevents the executable examples from drifting from that shape.
It contains no bearer token and remains entirely caller-owned.
