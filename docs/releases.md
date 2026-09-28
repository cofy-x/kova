# Releases

Every semantic-version tag publishes the cross-platform `kova` client and all
three Linux runtime roles from the same commit.

## Versioning

Tags use semantic versions such as `v0.1.0`. A suffix such as
`v0.1.0-rc.1` creates a prerelease and does not update stable image tags.
Kova is pre-1.0, so release notes may document intentional API changes.

Durable installation docs use `vX.Y.Z` instead of naming the newest release.
Users select an exact tag from GitHub Releases, and the chart, CLI, and runtime
images derive from that one value. Concrete versions belong in release notes,
asset names, checksums, and immutable deployment records.

## Published Artifacts

Each tag publishes:

- `kova` archives for Linux, macOS, and Windows on `amd64` and `arm64`
- the `kova-client` Python wheel and source distribution to PyPI and the GitHub release
- `checksums.txt`, a CycloneDX CLI SBOM, and build-provenance attestations
- `oci://ghcr.io/cofy-x/charts/kova` with a chart version matching the tag
- the packaged Helm chart, its OCI digest, and build-provenance attestation
- `ghcr.io/cofy-x/kova:controller-<version>`
- `ghcr.io/cofy-x/kova:runner-<version>`
- `ghcr.io/cofy-x/kova:worker-<version>`
- multi-platform OCI provenance and SBOM attestations for every image role
- `candidate-images.json`, the captured top-level digest for each validated role,
  covered by the release checksums

Stable releases also update `controller-latest`, `runner-latest`, and
`worker-latest`.

Install an exact CLI version with Go:

```bash
go install github.com/cofy-x/kova/cmd/kova@vX.Y.Z
```

Use an explicit version in automation. `@latest` is convenient for interactive
use but follows the version selected by the Go module proxy.

Install the matching Python SDK from PyPI:

```bash
python -m pip install "kova-client==X.Y.Z"
```

Python package versions use PEP 440, so a Kova tag such as `vX.Y.Z-rc.1` is published as `X.Y.Zrc1`.
The import package is `kova_client`.
PyPI publication uses OIDC trusted publishing through the protected, tag-only `pypi` GitHub environment and occurs only after the candidate Service and upgrade smoke succeeds.
The publishing job has `contents: read` for the publication guards and
`id-token: write` for trusted publishing; it does not use `PYPI_TOKEN` or another
long-lived registry credential. Wheel/sdist versions must match the selected tag.
Before upload, any already-existing PyPI file must match the validated artifact
hash; after upload, the public complete file set and a clean SDK installation
are checked. `skip-existing` is not permission to mix different build attempts.
The verifier snapshots each original wheel/sdist once, deriving the wheel identity and hash from the same bytes; it does not rebuild or modify these inputs while retrying.
The post-publication verifier gives the exact version up to 300 seconds to become visible, using a monotonic deadline and 5, 10, 20, then at most 30 seconds between attempts.
Each anonymous metadata request runs in an owned worker with a total timeout of at most 10 seconds or the remaining deadline, including DNS and response reads; an expired worker is killed and reaped.
Only HTTP 404 and recognized transient connection, timeout, or temporary DNS errors are retried.
Authentication/permission errors, other HTTP errors, certificate errors, invalid metadata, and version, complete file-set, SHA-256, duplicate, or yank mismatches stop immediately.
Attempt, status, elapsed time, remaining time, and next delay go to stderr; stdout remains only the verified PEP 440 version for the installation step.
The pre-upload check retains its absent-version and matching-subset semantics, with a separate 60-second transient-network deadline.
If visibility never becomes verifiable, the gate fails; rerun only the failed verification and dependants against the original validated artifacts, never rebuild, republish, or retag an immutable release as a visibility workaround.
The release workflow reserves seven minutes for this verification and clean installation, within the existing 15-minute published-smoke job.
The visibility deadline does not promise PyPI propagation or bound the later pip installation; the workflow step remains its outer timeout.
Registry environments are intentionally separate: `pypi` is only for the Python SDK, and any future TypeScript SDK must publish through its own `npm` environment.

## Release Gates

The tag workflow:

1. validates the semantic version and refuses to rebuild an existing GitHub release;
2. builds the six CLI archives and the Python wheel and source distribution, validates their contents, and generates the CLI SBOM;
3. packages and attests a candidate Helm chart while building and attesting
   controller, runner, and worker images for Linux `amd64` and `arm64`;
4. stops and drains the previous public Service in a kind cluster, applies and probes the candidate CRD, starts the candidate in a fresh runner namespace, runs the authenticated Service lifecycle, then drains it before rolling back to the baseline;
5. promotes the exact candidate digests used by smoke and publishes the OCI chart;
   candidate tags are isolated per workflow attempt, and an existing version
   image/chart must match rather than being overwritten with new content;
6. assembles checksums and verifies the CLI, anonymously pullable chart, role
   image boundaries, runtime users, and anonymous image pulls;
7. creates the GitHub release with installation guidance and generated change
   notes only after every blocking gate succeeds.

After a successful release, the release smoke downloads the public CLI and
checksum file and captured role digests, compares the anonymously pulled chart
against its checksummed release archive, pins role images by digest, and runs the
immutable-source Service lifecycle in a fresh kind cluster. It can also be
started manually for an exact tag. This catches registry availability and
packaging regressions that cannot be observed until the release is public.

Create a tag only from a commit whose required CI and CodeQL checks have
passed. The GHCR package must be public before the first tag workflow can pass
its anonymous-pull gate.

Do not rerun all jobs to overwrite a completed version. Use the separate
released-artifact smoke for revalidation. A partial publication can resume with
the original validated artifacts; digest/hash drift blocks it and requires
investigation or a new version, not replacement of a versioned object. Concrete
candidate notes under `docs/release-notes/<tag>.md` are included in the public
release and must state known issues, migration barriers and capacity caveats.
