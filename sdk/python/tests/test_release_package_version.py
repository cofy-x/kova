from __future__ import annotations

import importlib.util
import io
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

CHECKER_PATH = Path(__file__).resolve().parents[3] / "scripts/ci/check-python-package.py"
SPEC = importlib.util.spec_from_file_location("release_package_checker", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def _artifacts(directory: Path, wheel_version: str, sdist_version: str) -> None:
    metadata = (
        "Metadata-Version: 2.4\n"
        "Name: kova-client\n"
        "Version: {version}\n"
        "License-Expression: Apache-2.0\n"
        "Requires-Dist: httpx<1,>=0.27\n"
    )
    dist_info = f"kova_client-{wheel_version}.dist-info"
    with zipfile.ZipFile(directory / f"kova_client-{wheel_version}-py3-none-any.whl", "w") as wheel:
        for name in CHECKER.PACKAGE_FILES:
            wheel.writestr(name, "")
        for name in CHECKER.DIST_INFO_FILES:
            data = metadata.format(version=wheel_version) if name == "METADATA" else ""
            wheel.writestr(f"{dist_info}/{name}", data)
    root = f"kova_client-{sdist_version}"
    with tarfile.open(directory / f"{root}.tar.gz", "w:gz") as sdist:
        files = CHECKER.SDIST_ROOT_FILES | {f"src/{name}" for name in CHECKER.PACKAGE_FILES}
        for name in sorted(files):
            data = metadata.format(version=sdist_version).encode() if name == "PKG-INFO" else b""
            member = tarfile.TarInfo(f"{root}/{name}")
            member.size = len(data)
            sdist.addfile(member, io.BytesIO(data))


@pytest.mark.parametrize("tag,version", [("v0.1.0-rc.10", "0.1.0rc10"), ("v0.1.0", "0.1.0")])
def test_release_tag_normalization_accepts_matched_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tag: str, version: str
) -> None:
    _artifacts(tmp_path, version, version)
    monkeypatch.setattr(sys, "argv", [str(CHECKER_PATH), str(tmp_path), tag])
    CHECKER.main()


@pytest.mark.parametrize(
    "wheel_version,sdist_version",
    [("0.1.0rc9", "0.1.0rc10"), ("0.1.0rc10", "0.1.0rc9"), ("0.0.0.dev0", "0.0.0.dev0")],
)
def test_release_rejects_wrong_version_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    wheel_version: str,
    sdist_version: str,
) -> None:
    _artifacts(tmp_path, wheel_version, sdist_version)
    monkeypatch.setattr(sys, "argv", [str(CHECKER_PATH), str(tmp_path), "v0.1.0-rc.10"])
    with pytest.raises(SystemExit, match="does not match release"):
        CHECKER.main()


def test_without_release_tag_still_rejects_wheel_sdist_version_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _artifacts(tmp_path, "0.1.0rc10", "0.1.0rc9")
    monkeypatch.setattr(sys, "argv", [str(CHECKER_PATH), str(tmp_path)])
    with pytest.raises(SystemExit, match="versions disagree"):
        CHECKER.main()


def test_version_metadata_is_required() -> None:
    with pytest.raises(SystemExit, match="missing package version"):
        CHECKER.check_version(Path("missing-version.whl"), None, "0.1.0rc10")
