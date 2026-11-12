from collections.abc import Mapping
from typing import Literal, overload

from ._put import PutResult
from ._store import ObjectStore, S3Store

@overload
def copy(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: Literal[False] = False,
) -> None: ...
@overload
def copy(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: Literal[True],
) -> PutResult: ...
@overload
def copy(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: bool,
) -> PutResult | None:
    """Copy an object, optionally to another S3 store.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path

    Keyword Args:
        overwrite: If `True`, if there exists an object at the destination, it will
                be overwritten.

            If `False`: will copy only if destination is empty. Performs an atomic operation if the underlying object storage supports it. If atomic operations are not supported by the underlying object storage (like S3) it will return an error.

            Will return an error if the destination already has an object.

        destination_store: Optional destination S3 store. The original store remains
            the source. Each path is relative to its own store's prefix. Omit this
            to copy within the original store. Stores must use the same service
            endpoint; different regional endpoints or service aliases are not
            supported. The destination's credentials must read the source and write
            the destination. No data passes through Python. Extended copies use
            CopyObject (maximum 5 GB) and require `overwrite=True`.

        source_version_id: Optional S3 source version to copy.

        return_result: Return the destination ETag and version as a `PutResult`.
            Defaults to `False`, preserving the existing `None` return value.
            Supported only for S3 with `overwrite=True`.

        metadata: Replacement custom metadata for S3 copies. `None` preserves source
            metadata; an empty mapping removes all custom metadata. Keys are sent as
            `x-amz-meta-{key}`, so `content-type` is custom metadata and does not set
            the standard `Content-Type` header. S3's replace directive also resets
            standard metadata headers such as `Content-Type`, `Cache-Control`, and
            `Content-Disposition` unless the service supplies defaults. Metadata is
            not supported with `overwrite=False` or by non-S3 stores. Source object
            tags continue to be copied.

    """

@overload
async def copy_async(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: Literal[False] = False,
) -> None: ...
@overload
async def copy_async(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: Literal[True],
) -> PutResult: ...
@overload
async def copy_async(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
    destination_store: S3Store | None = None,
    source_version_id: str | None = None,
    return_result: bool,
) -> PutResult | None:
    """Call `copy` asynchronously.

    Refer to the documentation for [copy][obstore.copy].
    """
