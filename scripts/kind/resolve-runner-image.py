#!/usr/bin/env python3
"""Read a local Kind registry manifest and return its immutable reference.

Already digest-pinned inputs require no network. Tags are only accepted on the
documented loopback fixture registry; no credentials, proxies, redirects, Docker
configuration IDs, remote writes, or mutable-tag retries are used.
"""

import hashlib
import json
import re
import sys
import urllib.error
import urllib.request

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MEDIA_TYPES = (
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("registry redirects are not allowed")


def manifest_digest(body, headers):
    if not body or len(body) > MAX_MANIFEST_BYTES:
        raise ValueError("manifest size is outside the bounded fixture limit")
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    if headers.get("Docker-Content-Digest") != digest:
        raise ValueError("manifest bytes do not match the registry digest")
    value = json.loads(body)
    media_type = headers.get("Content-Type", "").split(";", 1)[0].strip()
    if value.get("schemaVersion") != 2 or media_type not in MEDIA_TYPES:
        raise ValueError("registry did not return an OCI/Docker v2 manifest or index")
    if value.get("mediaType") not in (None, media_type):
        raise ValueError("manifest media type disagrees with the registry")
    return digest


def resolve(image, fetch=None):
    if len(image) > 512:
        raise ValueError("runner image exceeds identity limit")
    if "@" in image:
        if not re.fullmatch(r"[^\s@]+/[^\s@]+@sha256:[0-9a-f]{64}", image):
            raise ValueError("runner image is not a canonical manifest reference")
        return image
    match = re.fullmatch(r"localhost:5002/([a-z0-9]+(?:[._/-][a-z0-9]+)*):([A-Za-z0-9_][A-Za-z0-9_.-]{0,127})", image)
    if not match:
        raise ValueError("remote runner images must be supplied as repository@sha256; only localhost:5002 fixture tags can be resolved")
    repository, tag = match.groups()
    if fetch is None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

        def fetch(reference):
            request = urllib.request.Request(
                "http://localhost:5002/v2/" + repository + "/manifests/" + reference,
                headers={"Accept": ", ".join(MEDIA_TYPES)},
            )
            with opener.open(request, timeout=10) as response:
                if response.status != 200:
                    raise ValueError("registry manifest read was not successful")
                return response.read(MAX_MANIFEST_BYTES + 1), response.headers

    body, headers = fetch(tag)
    digest = manifest_digest(body, headers)
    # Re-read by the digest, not by a possibly changing tag, and compare bytes.
    pinned_body, pinned_headers = fetch(digest)
    if manifest_digest(pinned_body, pinned_headers) != digest or pinned_body != body:
        raise ValueError("digest readback differs from the observed manifest")
    return "localhost:5002/" + repository + "@" + digest


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("one runner image reference is required")
        print(resolve(sys.argv[1]))
    except (ValueError, OSError, urllib.error.URLError) as error:
        print("runner manifest resolution STOP: " + str(error), file=sys.stderr)
        sys.exit(1)
