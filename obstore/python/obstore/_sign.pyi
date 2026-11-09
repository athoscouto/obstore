from collections.abc import Sequence
from datetime import timedelta
from typing import Literal, TypeAlias, TypedDict, overload

from .store import AzureStore, GCSStore, S3Store

HTTP_METHOD: TypeAlias = Literal[
    "GET",
    "PUT",
    "POST",
    "HEAD",
    "PATCH",
    "TRACE",
    "DELETE",
    "OPTIONS",
    "CONNECT",
]
"""Allowed HTTP Methods for signing."""

SignCapableStore: TypeAlias = AzureStore | GCSStore | S3Store
"""ObjectStore instances that are capable of signing."""

class SignedUrlOptions(TypedDict, total=False):
    """Query parameters and headers to bind into a signed URL.

    !!! warning "Not importable at runtime"

        Pass an ordinary dictionary at runtime. To use this type hint, import it
        within a `TYPE_CHECKING` block:

        ```py
        from __future__ import annotations
        from typing import TYPE_CHECKING
        if TYPE_CHECKING:
            from obstore import SignedUrlOptions
        ```
    """

    extra_query: Sequence[tuple[str, str]]
    """Additional query parameters, such as versionId or response overrides."""
    signed_headers: dict[str, str]
    """Headers the requester must send with the exact signed values."""

@overload
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> str: ...
@overload
def sign(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> Sequence[str]: ...
def sign(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> str | Sequence[str]:
    """Create a signed URL.

    Given the intended `method` and `paths` to use and the desired length of time for
    which the URL should be valid, return a signed URL created with the object store
    implementation's credentials such that the URL can be handed to something that
    doesn't have access to the object store's credentials, to allow limited access to
    the object store.

    Args:
        store: The ObjectStore instance to use.
        method: The HTTP method to use.
        paths: The path(s) within ObjectStore to retrieve. If
        expires_in: How long the signed URL(s) should be valid.
        options: Optional query parameters and headers to include in the signature.

    Returns:
        _description_

    """

@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str,
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> str: ...
@overload
async def sign_async(
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: Sequence[str],
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> Sequence[str]: ...
async def sign_async(  # type: ignore[misc] # docstring in pyi file
    store: SignCapableStore,
    method: HTTP_METHOD,
    paths: str | Sequence[str],
    expires_in: timedelta,
    *,
    options: SignedUrlOptions | None = None,
) -> str | Sequence[str]:
    """Call `sign` asynchronously.

    Refer to the documentation for [sign][obstore.sign].
    """
