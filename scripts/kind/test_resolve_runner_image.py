#!/usr/bin/env python3
"""Offline checks: no Docker, Kubernetes, or registry writes."""
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("resolver", Path(__file__).with_name("resolve-runner-image.py"))
resolver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(resolver)


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.body = json.dumps({"schemaVersion": 2, "mediaType": resolver.MEDIA_TYPES[0], "config": {"digest": "sha256:" + "b" * 64}}).encode()
        self.digest = "sha256:" + hashlib.sha256(self.body).hexdigest()
        self.headers = {"Docker-Content-Digest": self.digest, "Content-Type": resolver.MEDIA_TYPES[0]}

    def test_manifest_bytes_not_config_id_and_digest_readback(self):
        calls = []

        def fetch(ref):
            calls.append(ref)
            return self.body, self.headers

        self.assertEqual(resolver.resolve("localhost:5002/kova:runner-dev", fetch), "localhost:5002/kova@" + self.digest)
        self.assertEqual(calls, ["runner-dev", self.digest])

    def test_pinned_remote_needs_no_network(self):
        image = "ghcr.io/cofy-x/kova@sha256:" + "a" * 64
        self.assertEqual(resolver.resolve(image, lambda _: self.fail("unexpected network")), image)

    def test_remote_tag_and_invalid_digest_rejected_without_fetch(self):
        for image in ("ghcr.io/cofy-x/kova:runner", "localhost:5003/kova:runner", "kova:runner", "localhost:5002/kova@sha256:" + "A" * 64):
            with self.subTest(image=image), self.assertRaises(ValueError):
                resolver.resolve(image, lambda _: self.fail("unexpected network"))

    def test_bad_headers_and_replaced_digest_readback_fail_closed(self):
        with self.assertRaises(ValueError):
            resolver.manifest_digest(self.body, dict(self.headers, **{"Docker-Content-Digest": "sha256:" + "b" * 64}))
        calls = []

        def fetch(ref):
            calls.append(ref)
            return (self.body if len(calls) == 1 else self.body + b" "), self.headers

        with self.assertRaises(ValueError):
            resolver.resolve("localhost:5002/kova:runner-dev", fetch)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
