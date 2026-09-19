#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
sdk_dir="${repo_root}/sdk/python"
python_version=${KOVA_PYTHON_VERSION:-3.10}
requested_output=${1:-}
temporary_output=
temporary_install=$(mktemp -d)

if [[ -n "${requested_output}" ]]; then
  mkdir -p "${requested_output}"
  output_dir=$(cd "${requested_output}" && pwd)
else
  temporary_output=$(mktemp -d)
  output_dir=${temporary_output}
fi

cleanup() {
  if [[ -n "${temporary_output}" ]]; then
    rm -rf "${temporary_output}"
  fi
  rm -rf "${temporary_install}"
}
trap cleanup EXIT

uv sync --python "${python_version}" --directory "${sdk_dir}" --locked
uv run --python "${python_version}" --directory "${sdk_dir}" --locked ruff check . "${repo_root}/examples/python-service-receipt.py" "${repo_root}/scripts/ci/check-python-package.py"
uv run --python "${python_version}" --directory "${sdk_dir}" --locked ruff format --check . "${repo_root}/examples/python-service-receipt.py" "${repo_root}/scripts/ci/check-python-package.py"
uv run --python "${python_version}" --directory "${sdk_dir}" --locked mypy src/kova_client "${repo_root}/examples/python-service-receipt.py"
uv run --python "${python_version}" --directory "${sdk_dir}" --locked pytest
uv run --python "${python_version}" --directory "${sdk_dir}" --locked python -m build --outdir "${output_dir}"
uv run --python "${python_version}" --directory "${sdk_dir}" --locked python "${repo_root}/scripts/ci/check-python-package.py" "${output_dir}"

wheel=$(find "${output_dir}" -maxdepth 1 -type f -name 'kova_client-*.whl' -print -quit)
uv venv --no-project --python "${python_version}" "${temporary_install}/venv"
uv pip install --python "${temporary_install}/venv/bin/python" "${wheel}"
"${temporary_install}/venv/bin/python" -c 'from kova_client import AsyncKovaClient, KovaClient; assert AsyncKovaClient and KovaClient'
