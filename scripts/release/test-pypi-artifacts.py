from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "pypi_artifacts", Path(__file__).with_name("verify-pypi-artifacts.py")
)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PublicArtifactTests(unittest.TestCase):
    def test_preflight_allows_only_matching_subset(self) -> None:
        expected = {"wheel.whl": "a" * 64, "sdist.tar.gz": "b" * 64}
        payload = {
            "info": {"version": "0.1.0rc10"},
            "urls": [{"filename": "wheel.whl", "digests": {"sha256": "a" * 64}}],
        }
        module.verify_public(payload, "0.1.0rc10", expected, allow_subset=True)
        with self.assertRaises(ValueError):
            module.verify_public(payload, "0.1.0rc10", expected)
        payload["urls"][0]["digests"]["sha256"] = "c" * 64
        with self.assertRaises(ValueError):
            module.verify_public(payload, "0.1.0rc10", expected, allow_subset=True)

    def test_exact_files_required(self) -> None:
        expected = {"kova_client-0.1.0rc10.whl": "a" * 64, "kova_client-0.1.0rc10.tar.gz": "b" * 64}
        payload = {
            "info": {"version": "0.1.0rc10"},
            "urls": [
                {"filename": name, "digests": {"sha256": digest}, "yanked": False}
                for name, digest in expected.items()
            ],
        }
        module.verify_public(payload, "0.1.0rc10", expected)
        for mutation in ("digest", "missing", "extra", "version", "yanked"):
            import copy

            changed = copy.deepcopy(payload)
            if mutation == "digest":
                changed["urls"][0]["digests"]["sha256"] = "c" * 64
            elif mutation == "missing":
                changed["urls"].pop()
            elif mutation == "extra":
                changed["urls"].append(changed["urls"][0])
            elif mutation == "version":
                changed["info"]["version"] = "0.1.0rc9"
            else:
                changed["urls"][0]["yanked"] = True
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                module.verify_public(changed, "0.1.0rc10", expected)


if __name__ == "__main__":
    unittest.main()
