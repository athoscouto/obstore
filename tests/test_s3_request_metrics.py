"""Public S3 request counters count real transport attempts, not helper calls."""

from __future__ import annotations

import asyncio
import gc
import pickle
from collections import Counter
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from aiohttp import web

import obstore
from obstore.store import S3RequestMetrics, S3Store, from_url
from tests.streaming_https import run_smoke

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from obstore.store import S3Config

APIS = {
    "PutObject",
    "CreateMultipartUpload",
    "UploadPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "GetObject",
    "HeadObject",
    "CopyObject",
    "DeleteObject",
    "DeleteObjects",
    "ListObjectsV2",
    "GetObjectTagging",
    "PutObjectTagging",
    "Other",
}


class Receiver:
    """Owned HTTP receiver recording actual S3 requests."""

    def __init__(self) -> None:
        """Initialize isolated request counts and failure controls."""
        self.requests: Counter[str] = Counter()
        self.fail_once: set[str] = set()
        self.reject_parts = False
        self.aborted = asyncio.Event()

    async def handle(self, request: web.Request) -> web.Response:
        """Read a complete request and return the selected S3 fixture response."""
        await request.read()
        if request.path == "/credentials":
            self.requests["Credentials"] += 1
            return web.json_response(
                {
                    "AccessKeyId": "test-access",
                    "SecretAccessKey": "test-secret",
                    "Token": "fixture-token",
                    "Expiration": "2100-01-01T00:00:00Z",
                },
            )
        if "uploads" in request.query:
            api = "CreateMultipartUpload"
        elif "uploadId" in request.query:
            api = {
                "PUT": "UploadPart",
                "POST": "CompleteMultipartUpload",
                "DELETE": "AbortMultipartUpload",
            }[request.method]
        else:
            api = "PutObject"
        self.requests[api] += 1
        failure = 403 if self.reject_parts and api == "UploadPart" else None
        if api in self.fail_once:
            self.fail_once.remove(api)
            failure = 503
        if failure is not None:
            return web.Response(status=failure)
        if api == "CreateMultipartUpload":
            return web.Response(
                text="<InitiateMultipartUploadResult><UploadId>fixture-id</UploadId></InitiateMultipartUploadResult>",
                content_type="application/xml",
            )
        if api == "CompleteMultipartUpload":
            return web.Response(
                text='<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>',
                content_type="application/xml",
            )
        if api == "AbortMultipartUpload":
            self.aborted.set()
            return web.Response(status=204)
        return web.Response(headers={"ETag": '"part"'})


@pytest_asyncio.fixture
async def receiver(unused_tcp_port: int) -> AsyncIterator[tuple[Receiver, str]]:
    receiver = Receiver()
    app = web.Application(client_max_size=16 * 1024 * 1024)
    app.router.add_route("*", "/{path:.*}", receiver.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", unused_tcp_port).start()
        yield receiver, f"http://127.0.0.1:{unused_tcp_port}"
    finally:
        await runner.cleanup()


def make_store(
    endpoint: str,
    metrics: S3RequestMetrics | None = None,
    *,
    bucket: str = "bucket",
    prefix: str | None = None,
) -> S3Store:
    config: S3Config = {
        "endpoint": endpoint,
        "region": "us-east-1",
        "access_key_id": "test",
        "secret_access_key": "test",
    }
    return S3Store(
        bucket,
        prefix=prefix,
        config=config,
        client_options={"allow_http": True},
        request_metrics=metrics,
        retry_config={
            "max_retries": 2,
            "retry_timeout": timedelta(seconds=3),
            "backoff": {
                "init_backoff": timedelta(milliseconds=1),
                "max_backoff": timedelta(milliseconds=2),
            },
        },
    )


def test_empty_cumulative_snapshot_and_pickle() -> None:
    metrics = S3RequestMetrics()
    assert metrics.snapshot() == dict.fromkeys(APIS, 0)
    assert metrics.snapshot() == metrics.snapshot()
    assert pickle.loads(pickle.dumps(metrics)).snapshot() == metrics.snapshot()
    with pytest.raises(ValueError, match="only supported for S3"):
        from_url("memory://", request_metrics=metrics)


@pytest.mark.asyncio
async def test_single_part_retries_and_shared_handle(
    receiver: tuple[Receiver, str],
) -> None:
    server, endpoint = receiver
    metrics = S3RequestMetrics()
    server.fail_once.add("PutObject")
    first = make_store(endpoint, metrics, prefix="first")
    second = make_store(endpoint, metrics, bucket="other", prefix="second")
    await obstore.put_async(first, "private-key", b"first", use_multipart=False)
    await obstore.put_async(second, "private-key", b"second", use_multipart=False)
    assert metrics.snapshot()["PutObject"] == server.requests["PutObject"] == 3
    obstore.sign(first, "GET", "private-key", timedelta(minutes=1))
    assert metrics.snapshot()["PutObject"] == 3
    del first, second
    gc.collect()
    assert metrics.snapshot() == {**dict.fromkeys(APIS, 0), "PutObject": 3}
    fresh = pickle.loads(pickle.dumps(metrics))
    assert fresh.snapshot() == dict.fromkeys(APIS, 0)


@pytest.mark.asyncio
async def test_multipart_retries(receiver: tuple[Receiver, str]) -> None:
    server, endpoint = receiver
    metrics = S3RequestMetrics()
    server.fail_once.add("UploadPart")
    store = make_store(endpoint, metrics)
    await obstore.put_async(
        store,
        "private-key",
        b"x" * (6 * 1024 * 1024),
        use_multipart=True,
        chunk_size=5 * 1024 * 1024,
        max_concurrency=1,
    )
    expected = {
        "CreateMultipartUpload": 1,
        "UploadPart": 3,
        "CompleteMultipartUpload": 1,
    }
    assert server.requests == expected
    assert metrics.snapshot() == {**dict.fromkeys(APIS, 0), **expected}


@pytest.mark.asyncio
async def test_failed_part_and_abort_are_counted(
    receiver: tuple[Receiver, str],
) -> None:
    server, endpoint = receiver
    metrics = S3RequestMetrics()
    server.reject_parts = True
    store = make_store(endpoint, metrics)
    with pytest.raises(obstore.exceptions.BaseError):
        await obstore.put_async(
            store,
            "private-key",
            b"x" * (6 * 1024 * 1024),
            use_multipart=True,
            chunk_size=5 * 1024 * 1024,
            max_concurrency=1,
        )
    async with asyncio.timeout(3):
        await server.aborted.wait()
    snapshot = metrics.snapshot()
    assert snapshot["CreateMultipartUpload"] == 1
    assert snapshot["UploadPart"] == server.requests["UploadPart"]
    assert snapshot["UploadPart"] >= 1
    assert snapshot["AbortMultipartUpload"] == 1
    assert snapshot["CompleteMultipartUpload"] == 0


@pytest.mark.asyncio
async def test_opt_in_isolated_from_other_stores(
    receiver: tuple[Receiver, str],
) -> None:
    server, endpoint = receiver
    metrics = S3RequestMetrics()
    await obstore.put_async(
        make_store(endpoint),
        "unobserved",
        b"a",
        use_multipart=False,
    )
    assert metrics.snapshot() == dict.fromkeys(APIS, 0)
    store = S3Store.from_url(
        "s3://bucket/prefix",
        request_metrics=metrics,
        endpoint=endpoint,
        access_key_id="test",
        secret_access_key="test",  # noqa: S106 - fixture credentials
        client_options={"allow_http": True},
    )
    await obstore.put_async(store, "observed", b"b", use_multipart=False)
    assert metrics.snapshot()["PutObject"] == 1
    assert server.requests["PutObject"] == 2


@pytest.mark.asyncio
async def test_streaming_https_path_uses_same_transport_counters() -> None:
    metrics = S3RequestMetrics()
    await run_smoke(request_metrics=metrics)
    assert metrics.snapshot() == {**dict.fromkeys(APIS, 0), "PutObject": 5}


@pytest.mark.asyncio
async def test_credential_provider_requests_are_excluded(
    receiver: tuple[Receiver, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, endpoint = receiver
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    ):
        monkeypatch.delenv(name, raising=False)
    token = tmp_path / "token"
    token.write_text("fixture-auth")
    metrics = S3RequestMetrics()
    store = S3Store(
        "bucket",
        endpoint=endpoint,
        container_credentials_full_uri=f"{endpoint}/credentials",
        container_authorization_token_file=str(token),
        client_options={"allow_http": True},
        request_metrics=metrics,
    )
    await obstore.put_async(store, "object", b"payload", use_multipart=False)
    assert server.requests == {"Credentials": 1, "PutObject": 1}
    assert metrics.snapshot() == {**dict.fromkeys(APIS, 0), "PutObject": 1}
