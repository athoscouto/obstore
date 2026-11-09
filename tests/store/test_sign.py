from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import pytest
import requests

import obstore
from obstore.store import AzureStore, S3Store

if TYPE_CHECKING:
    from obstore import SignedUrlOptions
    from obstore.store import ClientConfig, S3Config


def test_sign_includes_prefix_in_url() -> None:
    """Signing must apply the store prefix to the path (regression for #682).

    This needs no live server: object_store builds the SigV4 presigned URL
    locally from the static credentials, so we can just inspect the path.
    """
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )
    url = str(obstore.sign(store, "GET", "filename.txt", timedelta(seconds=3600)))
    assert "/my-bucket/some/prefix/filename.txt?" in url


def test_sign_roundtrip_with_prefix(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    """End-to-end: a presigned GET for a prefixed store actually fetches the object.

    Before the fix, put() wrote to ``some/prefix/filename.txt`` but sign() pointed
    at ``filename.txt``, so this GET would 404.
    """
    s3_config, client_options = minio_bucket
    store = S3Store(
        config=s3_config,
        client_options=client_options,
        prefix="some/prefix",
    )

    store.put("filename.txt", b"hello world")

    url = str(obstore.sign(store, "GET", "filename.txt", timedelta(seconds=3600)))
    assert "/some/prefix/filename.txt?" in url

    resp = requests.get(url, timeout=30)
    assert resp.status_code == 200, resp.text
    assert resp.content == b"hello world"


def test_signed_header_is_enforced_by_minio(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    s3_config, client_options = minio_bucket
    store = S3Store(config=s3_config, client_options=client_options, prefix="signed")
    url = str(
        obstore.sign(
            store,
            "PUT",
            "header.txt",
            timedelta(hours=1),
            options={"signed_headers": {"content-type": "text/plain"}},
        ),
    )

    accepted = requests.put(
        url,
        data=b"signed",
        headers={"Content-Type": "text/plain"},
        timeout=30,
    )
    assert accepted.status_code == 200, accepted.text

    rejected = requests.put(
        url,
        data=b"altered",
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    assert rejected.status_code == 403, rejected.text


def test_signed_query_is_enforced_by_minio(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    s3_config, client_options = minio_bucket
    store = S3Store(config=s3_config, client_options=client_options)
    store.put("query.txt", b"query signature")
    url = str(
        obstore.sign(
            store,
            "GET",
            "query.txt",
            timedelta(hours=1),
            options={
                "extra_query": [
                    (
                        "response-content-disposition",
                        'attachment; filename="original.txt"',
                    ),
                ],
            },
        ),
    )

    accepted = requests.get(url, timeout=30)
    assert accepted.status_code == 200, accepted.text
    assert (
        accepted.headers["Content-Disposition"] == 'attachment; filename="original.txt"'
    )

    altered = requests.get(url.replace("original.txt", "altered.txt"), timeout=30)
    assert altered.status_code == 403, altered.text


def test_sign_batch_includes_prefix_in_urls() -> None:
    """Signing a list of paths applies the prefix to each (regression for #682).

    Exercises the batch ``signed_urls`` path rather than the single ``signed_url``.
    """
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )
    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(store, "GET", paths, timedelta(seconds=3600))
    assert isinstance(urls, list)
    assert len(urls) == len(paths)
    assert "/my-bucket/some/prefix/one.txt?" in str(urls[0])
    assert "/my-bucket/some/prefix/nested/two.txt?" in str(urls[1])


def test_sign_options_are_applied_to_single_and_batch_urls() -> None:
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )
    options: SignedUrlOptions = {
        "extra_query": [
            ("versionId", "v 1/2"),
            ("response-content-disposition", 'attachment; filename="a b.csv"'),
            ("response-content-type", "text/csv"),
            ("tag", "one"),
            ("tag", "two"),
        ],
        "signed_headers": {
            "content-type": "text/csv",
            "x-amz-checksum-sha256": "YQ==",
        },
    }
    urls = [
        obstore.sign(store, "GET", "one.txt", timedelta(hours=1), options=options),
        *obstore.sign(
            store,
            "GET",
            ["two.txt", "three.txt"],
            timedelta(hours=1),
            options=options,
        ),
    ]
    for url in urls:
        assert "/my-bucket/some/prefix/" in str(url)
        assert "versionId=v%201%2F2" in str(url)
        assert "response-content-type=text%2Fcsv" in str(url)
        assert "X-Amz-SignedHeaders=content-type%3Bhost%3Bx-amz-checksum-sha256" in str(
            url,
        )
        assert str(url).count("tag=") == 2


def test_sign_async_options_and_empty_options_compatibility() -> None:
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
        prefix="some/prefix",
    )

    async def run() -> tuple[str, list[str], str]:
        one = await obstore.sign_async(
            store,
            "GET",
            "one.txt",
            timedelta(hours=1),
            options={"extra_query": [("versionId", "v1")]},
        )
        many = await obstore.sign_async(
            store,
            "GET",
            ["two.txt", "three.txt"],
            timedelta(hours=1),
            options={"extra_query": [("response-content-type", "text/plain")]},
        )
        default = await obstore.sign_async(
            store,
            "GET",
            "four.txt",
            timedelta(hours=1),
            options={},
        )
        return str(one), [str(url) for url in many], str(default)

    one, many, default = asyncio.run(run())
    assert "versionId=v1" in one
    assert all("response-content-type=text%2Fplain" in url for url in many)
    assert "/my-bucket/some/prefix/four.txt?" in default


def test_sign_rejects_invalid_signed_header() -> None:
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
    )
    with pytest.raises(ValueError, match="invalid HTTP header name"):
        obstore.sign(
            store,
            "GET",
            "file.txt",
            timedelta(hours=1),
            options={"signed_headers": {"bad header": "value"}},
        )


def test_sign_rejects_unknown_options() -> None:
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
    )
    with pytest.raises(ValueError, match='Unknown signed URL option "signed_header"'):
        obstore.sign(
            store,
            "GET",
            "file.txt",
            timedelta(hours=1),
            options=cast(
                "SignedUrlOptions",
                {"signed_header": {"content-type": "text/plain"}},
            ),
        )


@pytest.mark.parametrize(
    "query",
    [[("X-Amz-Signature", "spoof")], [("", "empty-name")]],
)
def test_sign_rejects_reserved_or_malformed_query_options(
    query: list[tuple[str, str]],
) -> None:
    store = S3Store(
        "my-bucket",
        access_key_id="foo",
        secret_access_key="bar",  # noqa: S106
        region="us-east-1",
    )
    with pytest.raises(obstore.exceptions.BaseError):
        obstore.sign(
            store,
            "GET",
            "file.txt",
            timedelta(hours=1),
            options={"extra_query": query},
        )


def test_sign_propagates_unsupported_backend_options() -> None:
    store = AzureStore("container", account_name="account")
    with pytest.raises(
        obstore.exceptions.BaseError,
        match="does not support signing URLs",
    ):
        obstore.sign(
            store,
            "GET",
            "file.txt",
            timedelta(hours=1),
            options={"extra_query": [("versionId", "v1")]},
        )


def test_sign_batch_roundtrip_with_prefix(
    minio_bucket: tuple[S3Config, ClientConfig],
) -> None:
    """End-to-end batch: presigned GETs for a prefixed store fetch each object."""
    s3_config, client_options = minio_bucket
    store = S3Store(
        config=s3_config,
        client_options=client_options,
        prefix="some/prefix",
    )

    store.put("one.txt", b"first")
    store.put("nested/two.txt", b"second")

    paths = ["one.txt", "nested/two.txt"]
    urls = obstore.sign(store, "GET", paths, timedelta(seconds=3600))
    assert "/some/prefix/one.txt?" in str(urls[0])
    assert "/some/prefix/nested/two.txt?" in str(urls[1])

    for url, expected in zip(
        [str(u) for u in urls],
        [b"first", b"second"],
        strict=True,
    ):
        resp = requests.get(url, timeout=30)
        assert resp.status_code == 200, resp.text
        assert resp.content == expected
