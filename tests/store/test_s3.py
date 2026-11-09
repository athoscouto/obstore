# ruff: noqa: PGH003

import pickle
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from obstore.exceptions import BaseError, UnauthenticatedError
from obstore.store import S3Store, from_url


@pytest.mark.parametrize(
    "prefix",
    ["", "dir/", "dir/part", "nested/part", "a+b/%", "bad//segment", "empty-page"],
)
def test_literal_prefix_list_sends_prefix_and_continuation(prefix: str):
    requests: list[dict[str, list[str]]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            requests.append(query)
            if prefix == "empty-page" and query.get("continuation-token") is None:
                body = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>empty-page</Prefix><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken></ListBucketResult>"""
            elif query.get("continuation-token") == ["page-2"]:
                body = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>dir/part</Prefix><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>dir/partial/key</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag-2&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""
            else:
                body = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>dir/part</Prefix><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken><Contents><Key>dir/partial/first</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag-1&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""
            encoded = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        store = S3Store(
            "bucket",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            region="us-east-1",
            skip_signature=True,
            client_options={"allow_http": True},
            virtual_hosted_style_request=False,
            literal_prefix=True,
        )
        assert store.literal_prefix is True
        assert "literal_prefix=True" in repr(store)
        stream = store.list(prefix, chunk_size=1)
        assert requests == []
        if prefix == "empty-page":
            result = stream.collect()
        else:
            first_chunk = next(stream)
            assert len(requests) == 1
            result = [*first_chunk, *stream.collect()]
    finally:
        server.shutdown()
        thread.join()
        server.server_close()

    expected_paths = (
        ["dir/partial/key"]
        if prefix == "empty-page"
        else ["dir/partial/first", "dir/partial/key"]
    )
    assert [item["path"] for item in result] == expected_paths
    assert [item.get("prefix") for item in requests] == [[prefix], [prefix]]
    assert requests[0].get("continuation-token") is None
    assert requests[1].get("continuation-token") == ["page-2"]


def test_literal_prefix_list_propagates_s3_errors():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_error(403)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        store = S3Store(
            "bucket",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            region="us-east-1",
            skip_signature=True,
            client_options={"allow_http": True},
            virtual_hosted_style_request=False,
            literal_prefix=True,
        )
        with pytest.raises(BaseError):
            store.list().collect()
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize("prefix", [None, ""])
@pytest.mark.asyncio
async def test_literal_prefix_list_respects_s3_store_prefix(
    minio_store: S3Store,
    prefix: str | None,
):
    scoped_store = S3Store(
        prefix="listing-scope",
        config=minio_store.config,
        client_options=minio_store.client_options,
        retry_config=minio_store.retry_config,
        literal_prefix=True,
    )
    await scoped_store.put_async("partial/key", b"included")
    await minio_store.put_async("listing-scope-other/partial/key", b"excluded")

    objects = await scoped_store.list(prefix).collect_async()

    assert [item["path"] for item in objects] == ["partial/key"]


@pytest.mark.asyncio
async def test_literal_prefix_list_honors_scoped_offset(minio_store: S3Store):
    scoped_store = S3Store(
        prefix="offset-scope",
        config=minio_store.config,
        client_options=minio_store.client_options,
        literal_prefix=True,
    )
    for key in ("a", "b", "c"):
        await scoped_store.put_async(key, key.encode())

    objects = await scoped_store.list_async(offset="a").collect_async()

    assert [item["path"] for item in objects] == ["b", "c"]


def test_literal_prefix_option_is_preserved_by_pickle_and_from_url():
    store = S3Store(
        "bucket",
        prefix="scope",
        literal_prefix=True,
        skip_signature=True,
    )
    restored = pickle.loads(pickle.dumps(store))
    from_store_method = S3Store.from_url(
        "s3://bucket/scope",
        literal_prefix=True,
        skip_signature=True,
    )
    from_top_level = from_url(
        "s3://bucket/scope",
        literal_prefix=True,
        skip_signature=True,
    )

    assert restored == store
    assert restored.literal_prefix is True
    assert from_store_method.literal_prefix is True
    assert from_store_method.prefix == "scope"
    assert isinstance(from_top_level, S3Store)
    assert from_top_level.literal_prefix is True


def test_literal_prefix_option_requires_bool():
    with pytest.raises(TypeError):
        S3Store("bucket", literal_prefix="true")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="only supported for S3"):
        from_url("file:///", literal_prefix=True)


def test_default_list_prefix_keeps_path_validation():
    store = S3Store("bucket")
    with pytest.raises(ValueError, match="Could not parse path"):
        store.list("bad//segment")


@pytest.mark.asyncio
async def test_literal_prefix_option_preserves_default_list_semantics(
    minio_store: S3Store,
):
    await minio_store.put_async("dir/partial/object", b"literal")
    await minio_store.put_async("dir/partiality/object", b"sibling")
    literal_store = S3Store(
        config=minio_store.config,
        client_options=minio_store.client_options,
        literal_prefix=True,
    )

    default_result = await minio_store.list("dir/partial").collect_async()
    literal_result = await literal_store.list("dir/partial").collect_async()

    assert [obj["path"] for obj in default_result] == ["dir/partial/object"]
    assert [obj["path"] for obj in literal_result] == [
        "dir/partial/object",
        "dir/partiality/object",
    ]


@pytest.mark.asyncio
async def test_list_async(minio_store: S3Store):
    await minio_store.put_async("afile", b"hello world")

    list_result = await minio_store.list().collect_async()
    assert any("afile" in x["path"] for x in list_result)


@pytest.mark.asyncio
async def test_get_async(minio_store: S3Store):
    await minio_store.put_async("afile", b"hello world")

    resp = await minio_store.get_async("afile")
    buf = await resp.bytes_async()
    assert buf == b"hello world"


def test_construct_store_boolean_config():
    # Should allow boolean parameter
    S3Store("bucket", skip_signature=True)


def test_error_overlapping_config_kwargs():
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", config={"skip_signature": True}, skip_signature=True)

    # Also raises for variations of the same parameter
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store(
            "bucket",
            config={"aws_skip_signature": True},  # type: ignore
            skip_signature=True,
        )

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store(
            "bucket",
            config={"AWS_SKIP_SIGNATURE": True},  # type: ignore
            skip_signature=True,
        )


def test_overlapping_config_keys():
    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store(
            "bucket",
            config={"aws_skip_signature": True, "skip_signature": True},  # type: ignore
        )

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", aws_skip_signature=True, skip_signature=True)  # type: ignore

    with pytest.raises(BaseError, match="Duplicate key"):
        S3Store("bucket", AWS_SKIP_SIGNATURE=True, skip_signature=True)  # type: ignore


@pytest.mark.asyncio
async def test_from_url():
    store = from_url(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    _meta = await store.head_async("2024-01-01_performance_fixed_tiles.parquet")


@pytest.mark.parametrize(
    "url",
    [
        "https://account.r2.cloudflarestorage.com/bucket",
        "https://account.eu.r2.cloudflarestorage.com/bucket",
    ],
)
def test_r2_from_url(url: str):
    store = S3Store.from_url(url)
    endpoint = store.config.get("endpoint")
    assert endpoint is not None
    assert (".eu." in endpoint) == (".eu." in url)


def test_pickle():
    store = S3Store(
        "ookla-open-data",
        region="us-west-2",
        skip_signature=True,
    )
    restored = pickle.loads(pickle.dumps(store))
    _objects = next(restored.list())


def test_config_round_trip():
    store = S3Store.from_url(
        "s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1",
        region="us-west-2",
        skip_signature=True,
    )
    new_store = S3Store(
        config=store.config,
        prefix=store.prefix,
        client_options=store.client_options,
        retry_config=store.retry_config,
    )
    assert store.config == new_store.config
    assert store.prefix == new_store.prefix
    assert store.client_options == new_store.client_options
    assert store.retry_config == new_store.retry_config


def test_invalid_credential_provider():
    """Test that passing an invalid synchronous credential provider raises an error.

    instead of trying to await the value.
    """

    def credential_provider():
        return {"access_key_id": "str", "expires_at": datetime.now(UTC)}

    store = S3Store("bucket", credential_provider=credential_provider)  # type: ignore
    with pytest.raises(UnauthenticatedError):
        store.list().collect()


@pytest.mark.asyncio
async def test_invalid_credential_provider_async():
    async def credential_provider():
        return {"access_key_id": "str", "expires_at": datetime.now(UTC)}

    store = S3Store("bucket", credential_provider=credential_provider)  # type: ignore
    with pytest.raises(UnauthenticatedError):
        await store.list().collect_async()


def test_eq():
    store = S3Store("bucket", client_options={"timeout": "10s"})
    store2 = S3Store("bucket", client_options={"timeout": "10s"})
    store3 = S3Store("bucket")
    literal_store = S3Store("bucket", literal_prefix=True)
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3
    assert store != literal_store


@pytest.mark.asyncio
async def test_literal_prefix_empty_store_prefix_matches_unscoped_store(
    minio_store: S3Store,
):
    await minio_store.put_async("partial/key", b"included")
    store = S3Store(
        prefix="",
        config=minio_store.config,
        client_options=minio_store.client_options,
        retry_config=minio_store.retry_config,
        literal_prefix=True,
    )
    objects = await store.list("partial").collect_async()
    assert [item["path"] for item in objects] == ["partial/key"]
