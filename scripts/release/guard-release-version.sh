#!/usr/bin/env bash
set -euo pipefail
repository=${1:?usage: guard-release-version.sh OWNER/REPO TAG}
tag=${2:?usage: guard-release-version.sh OWNER/REPO TAG}
response=$(gh api --include "repos/${repository}/releases/tags/${tag}" 2>/dev/null) || true
status=$(printf '%s\n' "${response}" | awk 'NR == 1 {print $2}')
case "${status}" in
  404) exit 0 ;;
  200) echo 'release already exists; use the released-artifact smoke workflow, never rebuild/overwrite its version' >&2 ;;
  *) echo 'cannot establish whether this release exists; refusing publication' >&2 ;;
esac
exit 1
