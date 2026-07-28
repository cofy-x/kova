# Releases

Every semantic-version tag publishes the cross-platform `kova` client and all
three Linux runtime roles from the same commit.

## Versioning

Tags use semantic versions such as `v0.1.0`. A suffix such as
`v0.1.0-rc.1` creates a prerelease and does not update stable image tags.
Kova is pre-1.0, so release notes may document intentional API changes.

## Published Artifacts

Each tag publishes:

- `kova` archives for Linux, macOS, and Windows on `amd64` and `arm64`
- `checksums.txt`, a CycloneDX CLI SBOM, and build-provenance attestations
- `ghcr.io/cofy-x/kova:controller-<version>`
- `ghcr.io/cofy-x/kova:runner-<version>`
- `ghcr.io/cofy-x/kova:worker-<version>`
- multi-platform OCI provenance and SBOM attestations for every image role

Stable releases also update `controller-latest`, `runner-latest`, and
`worker-latest`.

Install an exact CLI version with Go:

```bash
go install github.com/cofy-x/kova/cmd/kova@v0.1.0
```

Use an explicit version in automation. `@latest` is convenient for interactive
use but follows the version selected by the Go module proxy.

## Release Gates

The tag workflow:

1. validates the semantic version;
2. builds the six CLI archives in parallel and generates checksums and SBOM;
3. builds and attests controller, runner, and worker images for Linux `amd64`
   and `arm64`;
4. verifies archive contents, the native CLI version, image role boundaries,
   runtime users, and anonymous pulls;
5. creates the GitHub release only after every smoke check succeeds.

Create a tag only from a commit whose required CI and CodeQL checks have
passed. The GHCR package must be public before the first tag workflow can pass
its anonymous-pull gate.
