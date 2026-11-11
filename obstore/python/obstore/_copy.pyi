from collections.abc import Mapping

from ._store import ObjectStore

def copy(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
) -> None:
    """Copy an object from one path to another in the same object store.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path

    Keyword Args:
        overwrite: If `True`, if there exists an object at the destination, it will
                be overwritten.

            If `False`: will copy only if destination is empty. Performs an atomic operation if the underlying object storage supports it. If atomic operations are not supported by the underlying object storage (like S3) it will return an error.

            Will return an error if the destination already has an object.

        metadata: Replacement custom metadata for S3 copies. `None` preserves source
            metadata; an empty mapping removes all custom metadata. Keys are sent as
            `x-amz-meta-{key}`, so `content-type` is custom metadata and does not set
            the standard `Content-Type` header. S3's replace directive also resets
            standard metadata headers such as `Content-Type`, `Cache-Control`, and
            `Content-Disposition` unless the service supplies defaults. Metadata is
            not supported with `overwrite=False` or by non-S3 stores. Source object
            tags continue to be copied.

    """

async def copy_async(
    store: ObjectStore,
    from_: str,
    to: str,
    *,
    overwrite: bool = True,
    metadata: Mapping[str, str] | None = None,
) -> None:
    """Call `copy` asynchronously.

    Refer to the documentation for [copy][obstore.copy].
    """
