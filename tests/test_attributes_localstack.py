"""Opt-in S3 custom metadata and system property roundtrips."""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import TYPE_CHECKING

import boto3
import pytest

from obstore.store import S3Store

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


@pytest.mark.skipif(
    not os.environ.get("OBSTORE_TEST_LOCALSTACK_ENDPOINT"),
    reason="set OBSTORE_TEST_LOCALSTACK_ENDPOINT to run LocalStack coverage",
)
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("use_multipart", [False, True])
@pytest.mark.parametrize("from_path", [False, True])
def test_s3_metadata_stays_separate_from_system_properties(
    tmp_path: Path,
    *,
    asynchronous: bool,
    use_multipart: bool,
    from_path: bool,
):
    endpoint = os.environ["OBSTORE_TEST_LOCALSTACK_ENDPOINT"]
    bucket = f"obstore-explicit-metadata-{uuid.uuid4().hex}"
    access_key_id = os.environ.get("LOCALSTACK_ACCESS_KEY_ID", "test")
    secret_access_key = os.environ.get("LOCALSTACK_SECRET_ACCESS_KEY", "test")
    sdk = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    sdk.create_bucket(Bucket=bucket)
    store = S3Store(
        bucket,
        endpoint=endpoint,
        region="us-east-1",
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )
    source: bytes | Path = b"payload"
    if from_path:
        source = tmp_path / "source.pdf"
        source.write_bytes(b"payload")
    kwargs: dict[str, Any] = {
        "attributes": {"Content-Type": "application/pdf", "Cache-Control": "private"},
        "metadata": {"content-type": "custom type", "cache-control": "custom cache"},
        "use_multipart": use_multipart,
    }
    try:
        if asynchronous:
            asyncio.run(store.put_async("document.pdf", source, **kwargs))
            result = asyncio.run(
                store.get_async("document.pdf", options={"head": True}),
            )
        else:
            store.put("document.pdf", source, **kwargs)
            result = store.get("document.pdf", options={"head": True})
        head = sdk.head_object(Bucket=bucket, Key="document.pdf")
        assert head["Metadata"] == kwargs["metadata"]
        assert head["ContentType"] == "application/pdf"
        assert head["CacheControl"] == "private"
        assert result.metadata == kwargs["metadata"]
        assert result.properties["Content-Type"] == "application/pdf"
        assert result.properties["Cache-Control"] == "private"
        assert bytes(store.get("document.pdf").bytes()) == b"payload"
    finally:
        sdk.delete_object(Bucket=bucket, Key="document.pdf")
        sdk.delete_bucket(Bucket=bucket)
