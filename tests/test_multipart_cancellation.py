from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import boto3
import pytest
from aiohttp import web

import obstore
from obstore.store import S3Store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from types_boto3_s3.type_defs import MultipartUploadTypeDef

    from obstore.store import ClientConfig, S3Config

pytestmark = pytest.mark.asyncio


class MultipartServer:
    """Gate multipart requests so cancellation stages are deterministic."""

    def __init__(self, stage: str, *, fail_abort: bool = False) -> None:
        """Initialize one isolated upload and its request gates."""
        self.stage = stage
        self.fail_abort = fail_abort
        self.part_started = asyncio.Event()
        self.complete_started = asyncio.Event()
        self.aborted = asyncio.Event()
        self.release = asyncio.Event()
        self.abort_count = 0
        self.completed = False

    async def handle(self, request: web.Request) -> web.Response:
        """Handle the minimal S3 multipart protocol used by the test."""
        await request.read()
        if request.method == "POST" and "uploads" in request.query:
            return web.Response(
                text=(
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket>"
                    "<Key>key</Key><UploadId>upload</UploadId>"
                    "</InitiateMultipartUploadResult>"
                ),
                content_type="application/xml",
            )
        if request.method == "PUT" and "partNumber" in request.query:
            self.part_started.set()
            if self.stage == "part":
                await self.release.wait()
            return web.Response(headers={"ETag": '"part"'})
        if request.method == "POST" and "uploadId" in request.query:
            self.complete_started.set()
            if self.stage == "complete":
                await self.release.wait()
            if self.aborted.is_set():
                return web.Response(
                    status=404,
                    text="<Error><Code>NoSuchUpload</Code></Error>",
                )
            self.completed = True
            return web.Response(
                text=(
                    "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key>"
                    '<ETag>"completed"</ETag></CompleteMultipartUploadResult>'
                ),
                content_type="application/xml",
            )
        if request.method == "DELETE" and request.query.get("uploadId") == "upload":
            self.abort_count += 1
            self.aborted.set()
            return web.Response(status=500 if self.fail_abort else 204)
        return web.Response(status=400)


@asynccontextmanager
async def multipart_server(
    stage: str,
    *,
    fail_abort: bool = False,
) -> AsyncIterator[tuple[MultipartServer, S3Store]]:
    server = MultipartServer(stage, fail_abort=fail_abort)
    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{path:.*}", server.handle)
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    store = S3Store(
        "bucket",
        endpoint=f"http://127.0.0.1:{port}",
        region="us-east-1",
        access_key_id="test",
        secret_access_key="test",  # noqa: S106
        client_options={"allow_http": True},
        retry_config={"max_retries": 0},
    )
    try:
        yield server, store
    finally:
        server.release.set()
        await runner.cleanup()


@pytest.mark.parametrize(
    ("stage", "body_kind"),
    [
        ("source", "stream"),
        ("part", "stream"),
        ("part", "path"),
        ("complete", "stream"),
        ("complete", "path"),
    ],
)
async def test_cancelled_upload_sends_abort(stage: str, body_kind: str, tmp_path: Path):
    async with multipart_server(stage) as (server, store):
        waiting_for_source = asyncio.Event()

        async def chunks() -> AsyncIterator[bytes]:
            yield b"data"
            if stage == "source":
                waiting_for_source.set()
                await server.release.wait()
            yield b"more"

        if body_kind == "path":
            body = tmp_path / "source.bin"
            body.write_bytes(b"datamore")
        else:
            body = chunks()
        pending = asyncio.ensure_future(
            obstore.put_async(
                store,
                "key",
                body,
                use_multipart=True,
                chunk_size=4,
                max_concurrency=1,
            ),
        )
        try:
            ready = {
                "source": waiting_for_source,
                "part": server.part_started,
                "complete": server.complete_started,
            }[stage]
            await asyncio.wait_for(ready.wait(), timeout=10)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(pending, timeout=1)
            await asyncio.wait_for(server.aborted.wait(), timeout=10)
            assert server.abort_count == 1
            assert not server.completed
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            server.release.set()


async def test_completed_upload_does_not_send_abort():
    async with multipart_server("success") as (server, store):
        result = await obstore.put_async(
            store,
            "key",
            b"datamore",
            use_multipart=True,
            chunk_size=4,
        )
        assert result["e_tag"] == '"completed"'
        assert server.completed
        assert server.abort_count == 0


@pytest.mark.parametrize("fail_abort", [False, True])
async def test_cleanup_preserves_producer_error(*, fail_abort: bool):
    async with multipart_server("source", fail_abort=fail_abort) as (server, store):

        async def chunks() -> AsyncIterator[bytes]:
            yield b"data"
            await server.part_started.wait()
            raise ValueError("producer failed")

        with pytest.raises(ValueError, match="producer failed"):
            await obstore.put_async(
                store,
                "key",
                chunks(),
                use_multipart=True,
                chunk_size=4,
            )
        await asyncio.wait_for(server.aborted.wait(), timeout=10)
        assert not server.completed


async def test_cancelled_upload_removes_server_multipart_state(
    minio_bucket: tuple[S3Config, ClientConfig],
):
    """Check cleanup through an independent S3 client, beyond the HTTP probe."""
    config, client_options = minio_bucket
    store = S3Store(config=config, client_options=client_options)
    endpoint = config.get("endpoint")
    bucket = config.get("bucket")
    assert endpoint is not None
    assert bucket is not None
    sdk = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=config.get("access_key_id"),
        aws_secret_access_key=config.get("secret_access_key"),
        region_name=config.get("region", "us-east-1"),
    )
    ready = asyncio.Event()
    release = asyncio.Event()
    key = "cancelled-upload.bin"

    async def chunks() -> AsyncIterator[bytes]:
        yield b"x" * (5 * 1024 * 1024)
        ready.set()
        await release.wait()

    async def uploads() -> list[MultipartUploadTypeDef]:
        response = await asyncio.to_thread(
            sdk.list_multipart_uploads,
            Bucket=bucket,
        )
        return [item for item in response.get("Uploads", []) if item.get("Key") == key]

    task = asyncio.ensure_future(
        obstore.put_async(store, key, chunks(), chunk_size=5 * 1024 * 1024),
    )
    try:
        await asyncio.wait_for(ready.wait(), timeout=10)
        assert len(await uploads()) == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        async with asyncio.timeout(10):
            # The independent S3 API exposes no event for abort completion.
            while await uploads():  # noqa: ASYNC110
                await asyncio.sleep(0.01)
        with pytest.raises(FileNotFoundError):
            await obstore.head_async(store, key)
    finally:
        task.cancel()
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        for upload in await uploads():
            upload_id = upload.get("UploadId")
            assert upload_id is not None
            await asyncio.to_thread(
                sdk.abort_multipart_upload,
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )
        sdk.close()
