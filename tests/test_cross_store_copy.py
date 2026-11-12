"""S3 server-side copies preserve each store's independent namespace."""

from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest

import obstore
from obstore.store import MemoryStore, S3Store

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any


@contextmanager
def copy_endpoint(
    *,
    status: int = 200,
    body: str | None = None,
) -> Iterator[tuple[str, list[tuple[str, dict[str, str]]]]]:
    requests: list[tuple[str, dict[str, str]]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            requests.append((self.path, dict(self.headers)))
            response = (
                body
                or '<CopyObjectResult><ETag>"copied-etag"</ETag></CopyObjectResult>'
            )
            payload = response.encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("x-amz-version-id", "destination-version")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def make_store(bucket: str, endpoint: str, *, prefix: str | None = None) -> S3Store:
    return S3Store(
        bucket,
        prefix=prefix,
        endpoint=endpoint,
        region="us-east-1",
        access_key_id=f"{bucket}-access-key",
        secret_access_key="test-secret",  # noqa: S106 - local mock credentials
        client_options={"allow_http": True},
        retry_config={"max_retries": 0},
        virtual_hosted_style_request=False,
    )


def run_copy(
    store: S3Store,
    source: str,
    target: str,
    *,
    asynchronous: bool,
    **kwargs: Any,  # noqa: ANN401
):
    if asynchronous:
        return asyncio.run(store.copy_async(source, target, **kwargs))
    return store.copy(source, target, **kwargs)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("metadata", [None, {}, {"content-type": "custom"}])
def test_cross_copy_uses_destination_transport_and_separate_prefixes(
    *,
    asynchronous: bool,
    metadata: dict[str, str] | None,
):
    with copy_endpoint() as (endpoint, requests):
        source = make_store("source", endpoint, prefix="incoming")
        destination = make_store("destination", endpoint, prefix="processed")
        result = run_copy(
            source,
            "café %.pdf",
            "same.pdf",
            asynchronous=asynchronous,
            destination_store=destination,
            source_version_id="version/+=?",
            metadata=metadata,
            return_result=True,
        )
    assert result == {"e_tag": '"copied-etag"', "version": "destination-version"}
    assert len(requests) == 1
    path, original_headers = requests[0]
    headers = {key.lower(): value for key, value in original_headers.items()}
    assert path == "/destination/processed/same.pdf"
    assert headers["x-amz-copy-source"] == (
        "source/incoming/caf%C3%A9%20%25.pdf?versionId=version%2F%2B%3D%3F"
    )
    assert "Credential=destination-access-key/" in headers["authorization"]
    if metadata is None:
        assert "x-amz-metadata-directive" not in headers
    else:
        assert headers["x-amz-metadata-directive"] == "REPLACE"
        assert headers.get("x-amz-meta-content-type") == metadata.get("content-type")
    assert "content-type" not in headers


def test_cross_copy_same_relative_key_and_default_none_return():
    with copy_endpoint() as (endpoint, requests):
        source = make_store("source", endpoint)
        destination = make_store("destination", endpoint)
        assert (
            source.copy("document.pdf", "document.pdf", destination_store=destination)
            is None
        )
    assert requests[0][0] == "/destination/document.pdf"
    assert requests[0][1]["x-amz-copy-source"] == "source/document.pdf"


def test_same_store_result_and_source_version_use_its_prefix():
    with copy_endpoint() as (endpoint, requests):
        source = make_store("source", endpoint, prefix="incoming")
        result = obstore.copy(
            source,
            "a",
            "b",
            source_version_id="old-version",
            return_result=True,
        )
    assert result is not None
    assert result["version"] == "destination-version"
    assert requests[0][0] == "/source/incoming/b"
    assert (
        requests[0][1]["x-amz-copy-source"] == "source/incoming/a?versionId=old-version"
    )


def test_extended_copy_rejects_unsupported_options_before_requests():
    with copy_endpoint() as (endpoint, requests):
        source = make_store("source", endpoint)
        destination = make_store("destination", endpoint)
        with pytest.raises(NotImplementedError, match="overwrite=True"):
            source.copy("a", "b", destination_store=destination, overwrite=False)
        with pytest.raises(NotImplementedError, match="S3 stores"):
            obstore.copy(MemoryStore(), "a", "b", destination_store=destination)
        with pytest.raises(NotImplementedError, match="S3 stores"):
            source.copy("a", "b", destination_store=MemoryStore())  # pyright: ignore[reportArgumentType]
        with pytest.raises(
            obstore.exceptions.NotSupportedError,
            match="same S3 service endpoint",
        ):
            source.copy(
                "a",
                "b",
                destination_store=make_store("other", "http://127.0.0.1:1"),
            )
    assert requests == []


@pytest.mark.parametrize(
    ("status", "body", "exception"),
    [
        (
            404,
            "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>",
            FileNotFoundError,
        ),
        (
            403,
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
            obstore.exceptions.PermissionDeniedError,
        ),
        (
            200,
            "<Error><Code>InternalError</Code><Message>failed</Message></Error>",
            obstore.exceptions.GenericError,
        ),
        (200, "<CopyObjectResult></CopyObjectResult>", obstore.exceptions.GenericError),
    ],
)
def test_copy_failures_are_not_successful_results(
    status: int,
    body: str,
    exception: type[Exception],
):
    with copy_endpoint(status=status, body=body) as (endpoint, requests):
        source = make_store("source", endpoint)
        destination = make_store("destination", endpoint)
        with pytest.raises(exception):
            source.copy("a", "b", destination_store=destination, return_result=True)
    assert len(requests) == 1


def test_existing_generic_copy_keeps_none_result_and_atomic_create():
    source = MemoryStore()
    source.put("source", b"source")
    assert source.copy("source", "destination", overwrite=False) is None
    with pytest.raises(obstore.exceptions.AlreadyExistsError):
        source.copy("source", "destination", overwrite=False)
    assert bytes(source.get("destination").bytes()) == b"source"
    with pytest.raises(NotImplementedError, match="S3 stores"):
        source.copy("source", "new", return_result=True)
    assert not source.list(prefix="new").collect()


def test_copy_rejects_different_effective_environment_endpoints(
    monkeypatch: pytest.MonkeyPatch,
):
    with copy_endpoint() as (endpoint, requests):
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", endpoint)
        source = S3Store("source", region="us-east-1")
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1")
        destination = S3Store("destination", region="us-east-1")
        with pytest.raises(
            obstore.exceptions.NotSupportedError,
            match="same S3 service endpoint",
        ):
            source.copy("a", "b", destination_store=destination)
    assert requests == []
