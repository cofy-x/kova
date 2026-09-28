#!/usr/bin/env bash
set -euo pipefail
script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "${test_dir}"' EXIT
digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
jq -n --arg digest "${digest}" '{controller:{"containerimage.digest":$digest},runner:{"containerimage.digest":$digest},worker:{"containerimage.digest":$digest}}' > "${test_dir}/metadata.json"
bash "${script_dir}/capture-image-digests.sh" "${test_dir}/metadata.json" > "${test_dir}/outputs"
test "$(wc -l < "${test_dir}/outputs" | tr -d ' ')" = 3
grep -Fx "worker_digest=${digest}" "${test_dir}/outputs" >/dev/null
for expression in 'del(.runner)' '.worker["containerimage.digest"]="sha256:no"' '.controller={"containerimage.config.digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'; do
  jq "${expression}" "${test_dir}/metadata.json" > "${test_dir}/bad.json"
  if bash "${script_dir}/capture-image-digests.sh" "${test_dir}/bad.json" > "${test_dir}/outputs" 2>/dev/null; then
    echo 'invalid candidate metadata was accepted' >&2
    exit 1
  fi
  test ! -s "${test_dir}/outputs"
done
echo 'candidate digest tests passed'
