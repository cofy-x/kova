# Releases

Kova publishes the cross-platform `kova` client and the Linux `kovad` runtime
from the same version tag. The client is a CGO-free workstation binary; the
runtime is delivered only in the container image.

## Versioning

Release tags use semantic versions such as `v0.1.0`. A suffix such as
`v0.1.0-rc.1` creates a prerelease and does not update the `latest` container
tag. Kova is pre-1.0, so minor versions may include intentional CLI or API
changes documented in the release notes.

## Published Artifacts

Each tag publishes:

- `kova` archives for Linux, macOS, and Windows on `amd64` and `arm64`
- a `checksums.txt` file covering every downloadable artifact
- a CycloneDX JSON SBOM for the CLI
- GitHub build-provenance attestations for the CLI artifacts
- `ghcr.io/cofy-x/kova:<version>` for Linux `amd64` and `arm64`
- OCI provenance and SBOM attestations attached to the runtime image

Stable tags also update `ghcr.io/cofy-x/kova:latest`.

## Release Flow

The release workflow runs only for version tags. It builds and attests the CLI
archives, publishes the multi-platform runtime image, verifies artifact
checksums, and then creates the GitHub release. Create a release tag only from
a commit whose required CI and CodeQL checks have passed.

GitHub Container Registry creates a new package as private on its first push.
For the first release, let the image job publish the package, change the
package visibility to public in the GitHub organization settings, and rerun
the failed release job. The release job verifies an anonymous image pull
before it creates the GitHub release, so later releases fail closed if package
visibility regresses.
