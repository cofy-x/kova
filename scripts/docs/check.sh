#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

python3 - "${ROOT}" <<'PY'
import pathlib
import re
import sys
import urllib.parse

root = pathlib.Path(sys.argv[1]).resolve()
link_pattern = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
errors = []

for document in sorted(root.rglob("*.md")):
    if ".git" in document.parts:
        continue
    text = document.read_text(encoding="utf-8")
    for target in link_pattern.findall(text):
        target = target.strip().split(maxsplit=1)[0].strip("<>")
        parsed = urllib.parse.urlsplit(target)
        if parsed.scheme or target.startswith("#"):
            continue
        relative = urllib.parse.unquote(parsed.path)
        if not relative:
            continue
        resolved = (document.parent / relative).resolve()
        if not resolved.exists():
            errors.append(f"{document.relative_to(root)}: missing link target {target}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)

print(f"checked {sum(1 for _ in root.rglob('*.md'))} Markdown files")
PY
