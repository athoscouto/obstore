"""Smoke-test the installed fork wheel without external services."""

from __future__ import annotations

import asyncio
import pickle
import runpy
import tomllib
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import obstore
from obstore.store import MemoryStore, S3Store

STREAM_RECEIVER = Path(__file__).resolve().parents[2] / "tests" / "streaming_https.py"


async def verify_copy_metadata() -> None:
    """Verify the new copy binding and request headers on every wheel platform."""
    requests: list[tuple[str, dict[str, str]]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self) -> None:
            requests.append((self.path, dict(self.headers.items())))
            body = b'<CopyObjectResult><ETag>"copy-smoke"</ETag></CopyObjectResult>'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *_args: object) -> None:  # noqa: A002
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        store = S3Store(
            "copy-smoke",
            prefix="scope",
            region="us-east-1",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            skip_signature=True,
            virtual_hosted_style_request=False,
            client_options={"allow_http": True},
        )
        cases = (None, {}, {"content-type": "custom", "origin": "wheel-smoke"})
        for metadata in cases:
            await store.copy_async("source", "destination", metadata=metadata)
            path, headers = requests[-1]
            headers = {key.lower(): value for key, value in headers.items()}
            assert path == "/copy-smoke/scope/destination"
            assert headers["x-amz-copy-source"] == "copy-smoke/scope/source"
            assert headers.get("x-amz-metadata-directive") == (
                None if metadata is None else "REPLACE"
            )
            assert {
                key.removeprefix("x-amz-meta-"): value
                for key, value in headers.items()
                if key.startswith("x-amz-meta-")
            } == (metadata or {})
        assert len(requests) == len(cases)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


async def main() -> None:
    """Exercise the installed fork's upload, listing-config and signing APIs."""
    # Exercise the installed binary through public put_async, with a loopback TLS
    # tunnel keeping the ordinary AWS hostname and certificate verification.
    await runpy.run_path(str(STREAM_RECEIVER))["run_smoke"](
        large_size=33 * 1024 * 1024 + 1,
    )
    await verify_copy_metadata()
    store = S3Store(
        "smoke-bucket",
        prefix="scope",
        literal_prefix=True,
        region="us-east-1",
        access_key_id="test",
        secret_access_key="test",  # noqa: S106
    )
    restored = pickle.loads(pickle.dumps(store))  # noqa: S301
    assert restored.literal_prefix is True
    assert S3Store("smoke-bucket").literal_prefix is False
    assert not hasattr(obstore, "list_paginated")
    url = await obstore.sign_async(
        restored,
        "PUT",
        "payload.bin",
        timedelta(minutes=1),
        options={
            "extra_query": [("partNumber", "1"), ("uploadId", "smoke-upload")],
            "signed_headers": {"content-type": "application/octet-stream"},
        },
    )
    parts = urlsplit(str(url))
    query = parse_qs(parts.query)
    assert parts.path.endswith("/smoke-bucket/scope/payload.bin")
    assert query["partNumber"] == ["1"]
    assert query["uploadId"] == ["smoke-upload"]
    assert "content-type" in query["X-Amz-SignedHeaders"][0]
    store = MemoryStore()
    with TemporaryDirectory() as directory:
        source = Path(directory) / "source.bin"
        for size in (1024 * 1024, 33 * 1024 * 1024):
            payload = b"x" * size
            source.write_bytes(payload)
            await obstore.put_async(
                store,
                "roundtrip.bin",
                source,
                multipart_threshold=32 * 1024 * 1024,
                chunk_size=8 * 1024 * 1024,
                max_concurrency=2,
            )
            result = await obstore.get_async(store, "roundtrip.bin")
            assert await result.bytes_async() == payload
        missing = Path(directory) / "missing.bin"
        try:
            await obstore.put_async(store, "missing.bin", missing)
        except FileNotFoundError as error:
            assert error.filename == str(missing)  # noqa: PT017
        else:
            raise AssertionError("Missing source must raise FileNotFoundError")


if __name__ == "__main__":
    manifest = tomllib.loads(Path("obstore/Cargo.toml").read_text())
    assert version("obstore") == manifest["package"]["version"]
    asyncio.run(main())
