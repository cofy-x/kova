from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone

import pytest
from conftest import FakeKovaService

import kova_client._client as client_module
from kova_client import (
    AsyncKovaClient,
    ClientConfig,
    CreateBuildRequest,
    JobStatus,
    KovaAPIError,
    KovaClient,
    KovaProtocolError,
    KovaWaitCancelled,
    OutputFormat,
    Platform,
    TargetSpec,
)
from kova_client._transport import parse_retry_after

DIGEST = "sha256:" + "a" * 64
CREATED_AT = "2026-09-19T00:00:00Z"


def job(status: str = "running") -> dict[str, object]:
    return {
        "id": "build-1",
        "status": status,
        "created_at": CREATED_AT,
        "requester": "test:caller",
        "source_uri": f"oci://registry.example.com/sources@{DIGEST}",
        "source_digest": DIGEST,
    }


def request() -> CreateBuildRequest:
    return CreateBuildRequest(
        source_uri=f"oci://registry.example.com/sources@{DIGEST}",
        source_digest=DIGEST,
        targets=(
            TargetSpec(
                target="registry.example.com/team/image:build-1",
                platform=Platform.LINUX_AMD64,
            ),
        ),
        concurrency=1,
        idempotency_key="build-1",
    )


def queue_lifecycle(service: FakeKovaService) -> None:
    service.json_response(
        "GET",
        "/version",
        {"api_version": "v1", "version": "v0.1.0", "commit": "abc", "build_date": CREATED_AT},
    )
    service.json_response("GET", "/readyz", {"status": "ready"})
    service.json_response("POST", "/v1/builds", job("queued"), status=202)
    service.json_response("GET", "/v1/builds", {"jobs": [job()]})
    service.json_response("GET", "/v1/builds/build-1", job("running"))
    service.json_response("GET", "/v1/builds/build-1", job("succeeded"))
    service.json_response(
        "GET",
        "/v1/builds/build-1/results",
        {
            "id": "build-1",
            "source_uri": f"oci://registry.example.com/sources@{DIGEST}",
            "source_digest": DIGEST,
            "idempotency_key": "build-1",
            "outputs": [
                {
                    "format": "oci",
                    "image": "registry.example.com/team/image:build-1",
                    "manifest_digest": DIGEST,
                    "immutable_ref": f"registry.example.com/team/image@{DIGEST}",
                    "platform": "linux/amd64",
                }
            ],
        },
    )
    service.text_response("GET", "/v1/builds/build-1/logs", "hello\n")
    service.json_response(
        "POST",
        "/v1/builds/build-1/cancel",
        {**job("running"), "cancellation_requested": True},
        status=202,
    )


def test_sync_client_lifecycle_uses_server_immutable_ref(fake_service: FakeKovaService) -> None:
    queue_lifecycle(fake_service)
    config = ClientConfig(base_url=fake_service.url, token="secret")
    with KovaClient(config) as client:
        assert client.version().api_version == "v1"
        assert client.ready().status == "ready"
        created = client.create_build(request())
        assert created.status is JobStatus.QUEUED
        assert len(client.list_builds().jobs) == 1
        terminal = client.wait_build("build-1", poll_interval=0.001, timeout=1)
        assert terminal.status is JobStatus.SUCCEEDED
        results = client.get_results("build-1")
        assert results.outputs[0].format is OutputFormat.OCI
        assert results.outputs[0].platform is Platform.LINUX_AMD64
        assert results.outputs[0].immutable_ref == f"registry.example.com/team/image@{DIGEST}"
        assert client.get_logs("build-1", tail_lines=10) == "hello\n"
        assert client.cancel_build("build-1").cancellation_requested

    post = next(item for item in fake_service.requests if item[0:2] == ("POST", "/v1/builds"))
    assert post[2]["Authorization"] == "Bearer secret"
    payload = json.loads(post[3])
    assert payload["idempotency_key"] == "build-1"
    assert payload["targets"] == [
        {"target": "registry.example.com/team/image:build-1", "platform": "linux/amd64"}
    ]
    public_requests = [item for item in fake_service.requests if item[1] in {"/version", "/readyz"}]
    assert all("Authorization" not in item[2] for item in public_requests)


def test_async_client_matches_sync_lifecycle(fake_service: FakeKovaService) -> None:
    queue_lifecycle(fake_service)

    async def exercise() -> None:
        async with AsyncKovaClient(
            ClientConfig(base_url=fake_service.url, token="secret")
        ) as client:
            assert (await client.version()).api_version == "v1"
            assert (await client.ready()).status == "ready"
            assert (await client.create_build(request())).status is JobStatus.QUEUED
            assert len((await client.list_builds()).jobs) == 1
            terminal = await client.wait_build("build-1", poll_interval=0.001, timeout=1)
            assert terminal.status is JobStatus.SUCCEEDED
            results = await client.get_results("build-1")
            assert results.outputs[0].immutable_ref == f"registry.example.com/team/image@{DIGEST}"
            assert await client.get_logs("build-1", tail_lines=10) == "hello\n"
            assert (await client.cancel_build("build-1")).cancellation_requested

    asyncio.run(exercise())


def test_create_build_is_not_retried_and_token_is_secret(fake_service: FakeKovaService) -> None:
    fake_service.json_response(
        "POST",
        "/v1/builds",
        {
            "code": "queue_capacity_exceeded",
            "message": "requester queue limit is reached",
            "retryable": True,
        },
        status=429,
        headers={"Retry-After": "7"},
    )
    config = ClientConfig(base_url=fake_service.url, token="do-not-leak")
    assert "do-not-leak" not in repr(config)
    with KovaClient(config) as client, pytest.raises(KovaAPIError) as caught:
        client.create_build(request())
    error = caught.value
    assert error.status_code == 429
    assert error.code == "queue_capacity_exceeded"
    assert error.message == "requester queue limit is reached"
    assert error.retryable
    assert error.retry_after == timedelta(seconds=7)
    assert "do-not-leak" not in str(error)
    assert sum(item[0:2] == ("POST", "/v1/builds") for item in fake_service.requests) == 1


def test_async_create_build_is_not_retried(fake_service: FakeKovaService) -> None:
    fake_service.json_response(
        "POST",
        "/v1/builds",
        {"code": "internal", "message": "temporarily unavailable", "retryable": True},
        status=503,
    )

    async def exercise() -> None:
        async with AsyncKovaClient(
            ClientConfig(base_url=fake_service.url, token="secret")
        ) as client:
            with pytest.raises(KovaAPIError):
                await client.create_build(request())

    asyncio.run(exercise())
    assert sum(item[0:2] == ("POST", "/v1/builds") for item in fake_service.requests) == 1


def test_wait_build_respects_retry_after(
    fake_service: FakeKovaService, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_service.json_response(
        "GET",
        "/v1/builds/build-1",
        {"code": "internal", "message": "temporarily unavailable", "retryable": True},
        status=503,
        headers={"Retry-After": "7"},
    )
    fake_service.json_response("GET", "/v1/builds/build-1", job("succeeded"))
    delays: list[float] = []

    def record_sleep(delay: float, _deadline: float | None, _event: threading.Event | None) -> None:
        delays.append(delay)

    monkeypatch.setattr(client_module, "_sync_sleep", record_sleep)
    with KovaClient(ClientConfig(base_url=fake_service.url, token="secret")) as client:
        assert client.wait_build("build-1", poll_interval=0.01).status is JobStatus.SUCCEEDED
    assert delays == [7.0]


def test_async_wait_build_respects_retry_after(
    fake_service: FakeKovaService, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_service.json_response(
        "GET",
        "/v1/builds/build-1",
        {"code": "internal", "message": "temporarily unavailable", "retryable": True},
        status=503,
        headers={"Retry-After": "5"},
    )
    fake_service.json_response("GET", "/v1/builds/build-1", job("succeeded"))
    delays: list[float] = []

    async def record_sleep(
        delay: float, _deadline: float | None, _event: asyncio.Event | None
    ) -> None:
        delays.append(delay)

    monkeypatch.setattr(client_module, "_async_sleep", record_sleep)

    async def exercise() -> None:
        async with AsyncKovaClient(
            ClientConfig(base_url=fake_service.url, token="secret")
        ) as client:
            terminal = await client.wait_build("build-1", poll_interval=0.01)
            assert terminal.status is JobStatus.SUCCEEDED

    asyncio.run(exercise())
    assert delays == [5.0]


def test_wait_build_timeout_and_cancellation(fake_service: FakeKovaService) -> None:
    fake_service.json_response("GET", "/v1/builds/build-1", job())
    fake_service.json_response("GET", "/v1/builds/build-1", job())
    with KovaClient(ClientConfig(base_url=fake_service.url, token="secret")) as client:
        with pytest.raises(TimeoutError):
            client.wait_build("build-1", timeout=0, poll_interval=1)
        event = threading.Event()
        event.set()
        with pytest.raises(KovaWaitCancelled):
            client.wait_build("build-1", cancel_event=event)

    async def exercise() -> None:
        async with AsyncKovaClient(
            ClientConfig(base_url=fake_service.url, token="secret")
        ) as client:
            with pytest.raises(TimeoutError):
                await client.wait_build("build-1", timeout=0, poll_interval=1)
            event = asyncio.Event()
            event.set()
            with pytest.raises(KovaWaitCancelled):
                await client.wait_build("build-1", cancel_event=event)

    asyncio.run(exercise())


def test_async_wait_propagates_task_cancellation(fake_service: FakeKovaService) -> None:
    fake_service.json_response("GET", "/v1/builds/build-1", job())

    async def exercise() -> None:
        async with AsyncKovaClient(
            ClientConfig(base_url=fake_service.url, token="secret")
        ) as client:
            task = asyncio.create_task(client.wait_build("build-1", poll_interval=60))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(exercise())


def test_from_env_is_explicit_and_parses_tls_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOVA_SERVICE_URL", "https://ignored.example.com")
    explicit = ClientConfig(base_url="https://kova.example.com", token="secret")
    assert explicit.base_url == "https://kova.example.com"
    config = ClientConfig.from_env(
        {
            "KOVA_SERVICE_URL": "https://service.example.com/prefix/",
            "KOVA_SERVICE_TOKEN": " token ",
            "KOVA_SERVICE_CA_FILE": "/tmp/kova-ca.pem",
            "KOVA_SERVICE_INSECURE": "false",
        }
    )
    assert config.base_url == "https://service.example.com/prefix"
    assert config.token == "token"
    assert config.ca_file == "/tmp/kova-ca.pem"
    assert not config.insecure


def test_sync_and_async_clients_bound_response_bodies(fake_service: FakeKovaService) -> None:
    fake_service.text_response("GET", "/v1/builds/build-1/logs", "x" * 17)
    fake_service.text_response("GET", "/v1/builds/build-1/logs", "x" * 17)
    config = ClientConfig(base_url=fake_service.url, token="secret", max_response_bytes=16)
    with KovaClient(config) as client, pytest.raises(KovaProtocolError, match="exceeds 16"):
        client.get_logs("build-1")

    async def exercise() -> None:
        async with AsyncKovaClient(config) as client:
            with pytest.raises(KovaProtocolError, match="exceeds 16"):
                await client.get_logs("build-1")

    asyncio.run(exercise())


def test_oversized_error_stays_typed(fake_service: FakeKovaService) -> None:
    fake_service.text_response("POST", "/v1/builds", "x" * ((1 << 20) + 1), status=503)
    with (
        KovaClient(ClientConfig(base_url=fake_service.url, token="secret")) as client,
        pytest.raises(KovaAPIError) as caught,
    ):
        client.create_build(request())
    assert caught.value.status_code == 503
    assert caught.value.code == "internal"
    assert caught.value.retryable


def test_rejects_inconsistent_server_immutable_reference(fake_service: FakeKovaService) -> None:
    other_digest = "sha256:" + "b" * 64
    fake_service.json_response(
        "GET",
        "/v1/builds/build-1/results",
        {
            "id": "build-1",
            "source_uri": f"oci://registry.example.com/sources@{DIGEST}",
            "source_digest": DIGEST,
            "outputs": [
                {
                    "format": "oci",
                    "image": "registry.example.com/team/image:build-1",
                    "manifest_digest": DIGEST,
                    "immutable_ref": f"registry.example.com/team/image@{other_digest}",
                    "platform": "linux/amd64",
                }
            ],
        },
    )
    with (
        KovaClient(ClientConfig(base_url=fake_service.url, token="secret")) as client,
        pytest.raises(KovaProtocolError),
    ):
        client.get_results("build-1")


def test_retry_after_http_date() -> None:
    now = datetime(2026, 9, 19, tzinfo=timezone.utc)
    assert parse_retry_after("Sat, 19 Sep 2026 00:00:05 GMT", now=now) == timedelta(seconds=5)
