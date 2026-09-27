#!/usr/bin/env python3
"""Create bounded invalid source archives for the isolated Kind rejection test."""

import argparse
import re
import stat
import zipfile
from pathlib import Path

SOURCE_LIMIT = 512 << 20
EXPANDED_LIMIT = 2 << 30
DOCKERFILE_LIMIT = 1 << 20
CHUNK = bytes(1 << 20)


def create_fixture(case: str, output: Path, tag: str) -> None:
    if not re.fullmatch(r"source-pressure-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}-[a-z-]+", tag):
        raise ValueError("tag must be an exact source-pressure run/case tag")
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    if case == "compressed-limit":
        # The CLI must reject the byte limit before treating these bytes as ZIP.
        with output.open("xb") as stream:
            stream.truncate(SOURCE_LIMIT + 1)
        return

    target = f"kind-registry:5000/kova-examples/source-pressure-rejection:{tag}"
    metadata = ('{"target":"' + target + '","platform":"linux/amd64"}').encode()
    with zipfile.ZipFile(
        output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=1, allowZip64=True
    ) as archive:
        dockerfile = (
            b"A" * (DOCKERFILE_LIMIT + 1) if case == "dockerfile-limit" else b"FROM scratch\n"
        )
        archive.writestr("image/Dockerfile", dockerfile)
        archive.writestr("image/metadata.json", metadata)
        if case == "symlink-parent":
            link = zipfile.ZipInfo("image/alias")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(link, b".")
            archive.writestr("image/alias/Dockerfile", b"FROM scratch\n")
        elif case == "expanded-limit":
            remaining = EXPANDED_LIMIT + 1
            with archive.open("image/payload", "w", force_zip64=True) as member:
                while remaining:
                    amount = min(remaining, len(CHUNK))
                    member.write(CHUNK[:amount])
                    remaining -= amount
        elif case != "dockerfile-limit":
            raise ValueError(f"unknown fixture case: {case}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        choices=("symlink-parent", "dockerfile-limit", "expanded-limit", "compressed-limit"),
        required=True,
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    create_fixture(args.case, args.output, args.tag)


if __name__ == "__main__":
    main()
