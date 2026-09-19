from __future__ import annotations

import email.parser
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

PACKAGE_FILES = {
    "kova_client/__init__.py",
    "kova_client/_client.py",
    "kova_client/_config.py",
    "kova_client/_errors.py",
    "kova_client/_models.py",
    "kova_client/_transport.py",
    "kova_client/_version.py",
    "kova_client/py.typed",
}
SDIST_ROOT_FILES = {".gitignore", "LICENSE", "PKG-INFO", "README.md", "pyproject.toml"}
DIST_INFO_FILES = {"METADATA", "RECORD", "WHEEL", "licenses/LICENSE"}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check-python-package.py DIST_DIRECTORY")
    directory = Path(sys.argv[1])
    wheels = list(directory.glob("kova_client-*.whl"))
    sdists = list(directory.glob("kova_client-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit("expected exactly one kova-client wheel and one source distribution")
    check_wheel(wheels[0])
    check_sdist(sdists[0])


def check_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        files = {name for name in archive.namelist() if not name.endswith("/")}
        package_files = {name for name in files if name.startswith("kova_client/")}
        if package_files != PACKAGE_FILES:
            fail(path, "package files", package_files, PACKAGE_FILES)
        metadata_paths = [name for name in files if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise SystemExit(f"{path}: expected one METADATA file")
        dist_info = str(PurePosixPath(metadata_paths[0]).parent)
        dist_files = {
            name.removeprefix(dist_info + "/") for name in files if name.startswith(dist_info + "/")
        }
        if dist_files != DIST_INFO_FILES:
            fail(path, "dist-info files", dist_files, DIST_INFO_FILES)
        metadata = email.parser.BytesParser().parsebytes(archive.read(metadata_paths[0]))
        if metadata["Name"] != "kova-client":
            raise SystemExit(f"{path}: unexpected package name {metadata['Name']!r}")
        if metadata["License-Expression"] != "Apache-2.0":
            raise SystemExit(f"{path}: missing Apache-2.0 license expression")
        if metadata.get_all("Requires-Dist") != ["httpx<1,>=0.27"]:
            raise SystemExit(f"{path}: unexpected runtime dependencies")


def check_sdist(path: Path) -> None:
    with tarfile.open(path) as archive:
        files = {member.name for member in archive.getmembers() if member.isfile()}
    roots = {PurePosixPath(name).parts[0] for name in files}
    if len(roots) != 1:
        raise SystemExit(f"{path}: source distribution has multiple roots")
    root = next(iter(roots))
    relative = {name.removeprefix(root + "/") for name in files}
    expected = SDIST_ROOT_FILES | {f"src/{name}" for name in PACKAGE_FILES}
    if relative != expected:
        fail(path, "source distribution files", relative, expected)


def fail(path: Path, label: str, actual: set[str], expected: set[str]) -> None:
    unexpected = sorted(actual - expected)
    missing = sorted(expected - actual)
    raise SystemExit(f"{path}: invalid {label}; unexpected={unexpected}, missing={missing}")


if __name__ == "__main__":
    main()
