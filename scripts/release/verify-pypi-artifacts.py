"""Fail closed unless public PyPI files match this run's wheel and sdist hashes."""

from __future__ import annotations

import email.parser
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


def local_artifacts(directory: Path) -> tuple[str, dict[str, str]]:
    wheels = list(directory.glob("kova_client-*.whl"))
    sdists = list(directory.glob("kova_client-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ValueError("expected one wheel and one sdist")
    with zipfile.ZipFile(wheels[0]) as archive:
        metadata_paths = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise ValueError("expected one wheel METADATA")
        metadata = email.parser.BytesParser().parsebytes(archive.read(metadata_paths[0]))
    version = metadata.get("Version", "")
    if metadata.get("Name") != "kova-client" or not re.fullmatch(r"[0-9A-Za-z.+!-]+", version):
        raise ValueError("invalid package identity")
    return version, {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in wheels + sdists
    }


def verify_public(
    payload: dict, version: str, expected: dict[str, str], *, allow_subset: bool = False
) -> None:
    if payload.get("info", {}).get("version") != version:
        raise ValueError("public package version differs")
    rows = payload.get("urls", [])
    actual = {row.get("filename"): row.get("digests", {}).get("sha256") for row in rows}
    matches = actual == expected
    if allow_subset:
        matches = all(
            name in expected and digest == expected[name] for name, digest in actual.items()
        )
    if len(rows) != len(actual) or not matches or any(row.get("yanked") for row in rows):
        raise ValueError(
            "public package files, hashes, or yank state differ from validated artifacts"
        )


def main() -> None:
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] != "--preflight"):
        raise SystemExit("usage: verify-pypi-artifacts.py DIST_DIRECTORY [--preflight]")
    preflight = len(sys.argv) == 3
    version, expected = local_artifacts(Path(sys.argv[1]))
    url = f"https://pypi.org/pypi/kova-client/{version}/json"
    for attempt in range(12):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                raw = response.read(2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024:
                raise ValueError("public package metadata exceeds size guard")
            verify_public(json.loads(raw), version, expected, allow_subset=preflight)
            print(version)
            return
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and preflight:
                print(version)
                return
            if exc.code != 404 or attempt == 11:
                raise
        except urllib.error.URLError:
            if attempt == 11:
                raise
        time.sleep(5)
    raise ValueError("public package did not appear")


if __name__ == "__main__":
    main()
