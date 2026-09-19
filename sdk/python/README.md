# kova-client

`kova-client` is the official, thin Python client for the Kova Service HTTP API v1.
It submits immutable sources for bounded image builds and returns server-verified manifest digests and immutable image references.
It does not own workflow recovery, artifact retention, datasets, episodes, or Axern/Axrun domain objects.

```bash
python -m pip install kova-client
```

```python
from kova_client import ClientConfig, CreateBuildRequest, KovaClient, Platform, TargetSpec

config = ClientConfig.from_env()
with KovaClient(config) as kova:
    job = kova.create_build(
        CreateBuildRequest(
            source_uri="oci://registry.example.com/team/sources@sha256:<manifest-digest>",
            source_digest="sha256:<source-content-digest>",
            targets=(
                TargetSpec(
                    target="registry.example.com/team/seed:build-123",
                    platform=Platform.LINUX_AMD64,
                ),
            ),
            concurrency=1,
            idempotency_key="build-123",
        )
    )
    terminal = kova.wait_build(job.id, timeout=600)
    if terminal.status == "succeeded":
        for output in kova.get_results(job.id).outputs:
            print(output.platform, output.immutable_ref, output.manifest_digest)
```

`ClientConfig.from_env()` explicitly reads `KOVA_SERVICE_URL`, `KOVA_SERVICE_TOKEN`, `KOVA_SERVICE_CA_FILE`, and `KOVA_SERVICE_INSECURE`.
Constructing `ClientConfig` or either client never reads a user directory or process environment implicitly.
Use a stable idempotency key if the caller may retry submission; `create_build` itself is never retried.
