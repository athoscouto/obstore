"""Opt-in S3 copy integration coverage against a running LocalStack."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import boto3
import pytest

from obstore.store import MemoryStore, S3Store

if TYPE_CHECKING:
    from types_boto3_s3.type_defs import HeadObjectOutputTypeDef, TagTypeDef

    from obstore.store import S3Config


@pytest.mark.skipif(
    not os.environ.get("OBSTORE_TEST_LOCALSTACK_ENDPOINT"),
    reason="set OBSTORE_TEST_LOCALSTACK_ENDPOINT to run LocalStack coverage",
)
def test_s3_copy_metadata_localstack():  # noqa: PLR0915
    endpoint = os.environ["OBSTORE_TEST_LOCALSTACK_ENDPOINT"]
    bucket = f"obstore-copy-metadata-{uuid.uuid4().hex}"
    sdk = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("LOCALSTACK_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("LOCALSTACK_SECRET_ACCESS_KEY", "test"),
    )
    sdk.create_bucket(Bucket=bucket)
    store = S3Store(
        bucket,
        endpoint=endpoint,
        region="us-east-1",
        access_key_id=os.environ.get("LOCALSTACK_ACCESS_KEY_ID", "test"),
        secret_access_key=os.environ.get("LOCALSTACK_SECRET_ACCESS_KEY", "test"),
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )

    def head(key: str) -> HeadObjectOutputTypeDef:
        return sdk.head_object(Bucket=bucket, Key=key)

    def body(key: str) -> bytes:
        return sdk.get_object(Bucket=bucket, Key=key)["Body"].read()

    def tags(key: str) -> list[TagTypeDef]:
        return sdk.get_object_tagging(Bucket=bucket, Key=key)["TagSet"]

    try:
        sdk.put_object(
            Bucket=bucket,
            Key="source",
            Body=b"payload",
            Metadata={"keep": "source", "content-type": "old"},
            Tagging="team=legal&kind=case",
            ContentType="text/plain",
            CacheControl="max-age=600",
            ContentDisposition="attachment",
        )
        source_metadata = head("source")["Metadata"].copy()
        source_headers = {
            field: head("source").get(field)
            for field in (
                "ContentType",
                "CacheControl",
                "ContentDisposition",
                "ContentLength",
            )
        }
        source_tags = tags("source")
        source_body = body("source")

        store.copy("source", "ordinary")
        sdk.copy_object(
            Bucket=bucket,
            Key="ordinary-sdk",
            CopySource={"Bucket": bucket, "Key": "source"},
            MetadataDirective="COPY",
            TaggingDirective="COPY",
        )
        assert body("ordinary") == body("ordinary-sdk")
        assert head("ordinary")["Metadata"] == {
            "keep": "source",
            "content-type": "old",
        }
        assert head("ordinary")["ContentType"] == "text/plain"
        assert head("ordinary")["CacheControl"] == "max-age=600"
        assert head("ordinary").get("ContentDisposition") == head("source").get(
            "ContentDisposition",
        )
        assert head("ordinary")["ContentLength"] == head("source")["ContentLength"]
        assert tags("ordinary") == [
            {"Key": "team", "Value": "legal"},
            {"Key": "kind", "Value": "case"},
        ]

        store.copy("source", "empty", metadata={})
        sdk.copy_object(
            Bucket=bucket,
            Key="empty-sdk",
            CopySource={"Bucket": bucket, "Key": "source"},
            MetadataDirective="REPLACE",
            Metadata={},
            TaggingDirective="COPY",
        )
        assert body("empty") == body("source")
        assert head("empty")["Metadata"] == {}
        assert head("empty")["ContentType"] == head("empty-sdk")["ContentType"]
        assert head("empty").get("CacheControl") == head("empty-sdk").get(
            "CacheControl",
        )
        assert tags("empty") == [
            {"Key": "team", "Value": "legal"},
            {"Key": "kind", "Value": "case"},
        ]

        store.copy(
            "source",
            "replaced",
            metadata={"New-Key": "new", "content-type": "new-type"},
        )
        sdk.copy_object(
            Bucket=bucket,
            Key="replaced-sdk",
            CopySource={"Bucket": bucket, "Key": "source"},
            MetadataDirective="REPLACE",
            Metadata={"new-key": "new", "content-type": "new-type"},
            TaggingDirective="COPY",
        )
        assert body("replaced") == body("source")
        assert head("replaced")["Metadata"] == {
            "new-key": "new",
            "content-type": "new-type",
        }
        assert head("replaced")["ContentType"] == head("replaced-sdk")["ContentType"]
        assert head("replaced").get("CacheControl") == head("replaced-sdk").get(
            "CacheControl",
        )
        assert head("replaced").get("ContentDisposition") == head("replaced-sdk").get(
            "ContentDisposition",
        )
        assert head("replaced")["ContentLength"] == head("source")["ContentLength"]
        assert tags("replaced") == [
            {"Key": "team", "Value": "legal"},
            {"Key": "kind", "Value": "case"},
        ]

        assert body("source") == source_body
        assert head("source")["Metadata"] == source_metadata
        assert {
            field: head("source").get(field) for field in source_headers
        } == source_headers
        assert tags("source") == source_tags

        with pytest.raises(FileNotFoundError):
            store.copy("missing", "missing-target", metadata={"x": "y"})
        assert (
            sdk.list_objects_v2(Bucket=bucket, Prefix="missing-target")["KeyCount"] == 0
        )

        with pytest.raises(NotImplementedError):
            store.copy("source", "overwrite-false", overwrite=False, metadata={})
        assert (
            sdk.list_objects_v2(Bucket=bucket, Prefix="overwrite-false")["KeyCount"]
            == 0
        )

        memory = MemoryStore()
        memory.put("source", b"payload")
        with pytest.raises(NotImplementedError):
            memory.copy("source", "destination", metadata={})
        assert [item["path"] for item in memory.list().collect()] == ["source"]
    finally:
        for item in sdk.list_objects_v2(Bucket=bucket).get("Contents", []):
            if key := item.get("Key"):
                sdk.delete_object(Bucket=bucket, Key=key)
        sdk.delete_bucket(Bucket=bucket)


@pytest.mark.skipif(
    not os.environ.get("OBSTORE_TEST_LOCALSTACK_ENDPOINT"),
    reason="set OBSTORE_TEST_LOCALSTACK_ENDPOINT to run LocalStack coverage",
)
@pytest.mark.parametrize("metadata", [None, {}, {"content-type": "replacement"}])
def test_cross_store_copy_preserves_sdk_contracts(
    metadata: dict[str, str] | None,
):
    endpoint = os.environ["OBSTORE_TEST_LOCALSTACK_ENDPOINT"]
    source_bucket = f"obstore-copy-source-{uuid.uuid4().hex}"
    destination_bucket = f"obstore-copy-destination-{uuid.uuid4().hex}"
    sdk = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("LOCALSTACK_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("LOCALSTACK_SECRET_ACCESS_KEY", "test"),
    )
    buckets = (source_bucket, destination_bucket)
    for bucket in buckets:
        sdk.create_bucket(Bucket=bucket)
        sdk.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
    options: S3Config = {
        "endpoint": endpoint,
        "region": "us-east-1",
        "access_key_id": os.environ.get("LOCALSTACK_ACCESS_KEY_ID", "test"),
        "secret_access_key": os.environ.get("LOCALSTACK_SECRET_ACCESS_KEY", "test"),
        "virtual_hosted_style_request": False,
    }
    source = S3Store(
        source_bucket,
        prefix="incoming",
        config=options,
        client_options={"allow_http": True},
    )
    destination = S3Store(
        destination_bucket,
        prefix="processed",
        config=options,
        client_options={"allow_http": True},
    )
    key = "document ação 50%.pdf"
    source_key = f"incoming/{key}"
    destination_key = f"processed/{key}"
    try:
        first = sdk.put_object(
            Bucket=source_bucket,
            Key=source_key,
            Body=b"original-version",
            Metadata={"content-type": "custom-type", "source": "original"},
            ContentType="application/pdf",
            CacheControl="private",
            Tagging="team=legal",
        )
        sdk.put_object(Bucket=source_bucket, Key=source_key, Body=b"newer-version")
        source_version = first["VersionId"]
        result = source.copy(
            key,
            key,
            destination_store=destination,
            source_version_id=source_version,
            metadata=metadata,
            return_result=True,
        )
        control_key = "sdk-control"
        if metadata is None:
            sdk.copy_object(
                Bucket=destination_bucket,
                Key=control_key,
                CopySource={
                    "Bucket": source_bucket,
                    "Key": source_key,
                    "VersionId": source_version,
                },
                MetadataDirective="COPY",
                TaggingDirective="COPY",
            )
        else:
            sdk.copy_object(
                Bucket=destination_bucket,
                Key=control_key,
                CopySource={
                    "Bucket": source_bucket,
                    "Key": source_key,
                    "VersionId": source_version,
                },
                MetadataDirective="REPLACE",
                Metadata=metadata,
                TaggingDirective="COPY",
            )
        actual = sdk.head_object(Bucket=destination_bucket, Key=destination_key)
        expected = sdk.head_object(Bucket=destination_bucket, Key=control_key)
        assert result is not None
        assert result["version"] == actual["VersionId"]
        assert result["e_tag"] == actual["ETag"]
        assert (
            sdk.get_object(Bucket=destination_bucket, Key=destination_key)[
                "Body"
            ].read()
            == b"original-version"
        )
        for field in ("ContentType", "ContentLength", "CacheControl", "Metadata"):
            assert actual.get(field) == expected.get(field)
        assert (
            sdk.get_object_tagging(Bucket=destination_bucket, Key=destination_key)[
                "TagSet"
            ]
            == sdk.get_object_tagging(Bucket=destination_bucket, Key=control_key)[
                "TagSet"
            ]
        )
        assert (
            sdk.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()
            == b"newer-version"
        )
    finally:
        for bucket in buckets:
            versions = sdk.list_object_versions(Bucket=bucket)
            for item in [
                *versions.get("Versions", []),
                *versions.get("DeleteMarkers", []),
            ]:
                sdk.delete_object(
                    Bucket=bucket,
                    Key=item["Key"],
                    VersionId=item["VersionId"],
                )
            sdk.delete_bucket(Bucket=bucket)
