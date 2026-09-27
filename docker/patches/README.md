# Nydusify Push Digest Patch

Kova builds the runner's `nydusify` binary from the pinned Nydus v2.4.4 source revision `4e908d8c7b6db8e5cac25ecf8b8cc1113dbacccb` and its SHA-256 checked source archive.
The upstream code and the Kova patch are Apache-2.0 licensed; the runner image includes the upstream `LICENSE-APACHE` file.

Upstream Nydusify's `--output-json` contains conversion metrics but not the target manifest digest.
The small patch wraps the converter's provider and records the exact target descriptor only after `Push` returns success, then adds `target_manifest_digest` to the JSON output.
Kova rejects a missing or malformed digest and verifies the output by its digest reference, never by rereading the mutable target tag.
`nydusify-push-digest_test.go.txt` is copied into the pinned source build as Go test code and tests interleaved same-tag pushes and failed pushes before the binary is built.

When updating Nydusify, pin the new source revision and archive checksum, rebase or remove the patch, and prove that the chosen binary reports the exact target digest after a successful push.
Run Go unit tests, build the runner image for both supported architectures, and run OCI/Nydus runtime E2E with concurrent same-tag writers before accepting an upgrade.
This source build adds a separate cached Go compilation step to runner-image builds; it does not add runtime archive copies or staging registry tags.
