#!/usr/bin/env bash
set -euo pipefail
script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "${test_dir}"' EXIT
mkdir "${test_dir}/bin"
cat > "${test_dir}/bin/gh" <<'MOCK'
#!/usr/bin/env bash
printf 'HTTP/2.0 %s\n' "${MOCK_STATUS:-500}"
[[ "${MOCK_STATUS:-500}" == 200 ]]
MOCK
cat > "${test_dir}/bin/docker" <<'MOCK'
#!/usr/bin/env bash
case "${MOCK_IMAGE:-error}" in
  missing) printf 'ERROR: %s: not found\n' "$4" >&2; exit 1 ;;
  ok) printf '{"digest":"%s"}\n' "${MOCK_DIGEST}" ;;
  *) echo 'network unavailable' >&2; exit 1 ;;
esac
MOCK
chmod +x "${test_dir}/bin/gh" "${test_dir}/bin/docker"
export PATH="${test_dir}/bin:${PATH}"
MOCK_STATUS=404 bash "${script_dir}/guard-release-version.sh" cofy-x/kova v0.1.0-rc.10
for status in 200 403 500; do
  if MOCK_STATUS="${status}" bash "${script_dir}/guard-release-version.sh" cofy-x/kova v0.1.0-rc.10 2>/dev/null; then exit 1; fi
done
digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
image=ghcr.io/cofy-x/kova:controller-v0.1.0-rc.10
MOCK_IMAGE=missing bash "${script_dir}/guard-image-version.sh" "${image}" "${digest}"
MOCK_IMAGE=ok MOCK_DIGEST="${digest}" bash "${script_dir}/guard-image-version.sh" "${image}" "${digest}"
for mode in error drift; do
  if [[ "${mode}" == drift ]]; then mock_mode=ok; else mock_mode=error; fi
  if MOCK_IMAGE="${mock_mode}" MOCK_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb bash "${script_dir}/guard-image-version.sh" "${image}" "${digest}" 2>/dev/null; then exit 1; fi
done
echo 'publication guards passed'
