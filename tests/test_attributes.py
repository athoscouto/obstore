from __future__ import annotations

import asyncio
from collections import UserDict
from typing import TYPE_CHECKING

import pytest

from obstore.store import MemoryStore

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


def test_content_type():
    store = MemoryStore()
    store.put("test.txt", b"Hello, World!", attributes={"Content-Type": "text/plain"})
    result = store.get("test.txt")
    assert result.attributes.get("Content-Type") == "text/plain"


def test_custom_attribute():
    store = MemoryStore()
    store.put(
        "test.txt",
        b"Hello, World!",
        attributes={"My-Custom-Attribute": "CustomValue"},
    )
    result = store.get("test.txt")
    assert result.attributes.get("My-Custom-Attribute") == "CustomValue"


@pytest.mark.parametrize("use_multipart", [False, True])
@pytest.mark.parametrize("from_path", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_explicit_metadata_preserves_system_property_collisions(
    tmp_path: Path,
    *,
    use_multipart: bool,
    from_path: bool,
    asynchronous: bool,
):
    store = MemoryStore()
    source: bytes | Path = b"payload"
    if from_path:
        source = tmp_path / "source.bin"
        source.write_bytes(b"payload")
    kwargs: dict[str, Any] = {
        "attributes": {
            "Content-Type": "application/pdf",
            "Cache-Control": "private",
            "source": "legacy",
        },
        "metadata": UserDict(
            {
                "Content-Type": "custom type",
                "content-type": "another custom type",
                "Cache-Control": "custom cache",
                "source": "explicit",
            },
        ),
        "use_multipart": use_multipart,
    }
    if asynchronous:
        asyncio.run(store.put_async("document.pdf", source, **kwargs))
        result = asyncio.run(store.get_async("document.pdf"))
    else:
        store.put("document.pdf", source, **kwargs)
        result = store.get("document.pdf")
    assert result.metadata == {
        "Content-Type": "custom type",
        "content-type": "another custom type",
        "Cache-Control": "custom cache",
        "source": "explicit",
    }
    assert result.properties == {
        "Content-Type": "application/pdf",
        "Cache-Control": "private",
    }
    assert bytes(result.bytes()) == b"payload"
    # Attribute views remain available after consuming the response.
    assert result.metadata["Content-Type"] == "custom type"
    assert result.properties["Content-Type"] == "application/pdf"


def test_explicit_empty_metadata_preserves_legacy_attributes():
    store = MemoryStore()
    store.put(
        "test.txt",
        b"Hello, World!",
        attributes={"Content-Type": "text/plain", "custom": "value"},
        metadata={},
    )
    result = store.get("test.txt")
    assert result.attributes == {"Content-Type": "text/plain", "custom": "value"}
    assert result.metadata == {"custom": "value"}
    assert result.properties == {"Content-Type": "text/plain"}


@pytest.mark.asyncio
async def test_metadata_head_and_async_iterator():
    store = MemoryStore()

    async def chunks():
        yield b"one"
        yield b"two"

    await store.put_async(
        "stream",
        chunks(),
        attributes={"Content-Type": "application/octet-stream"},
        metadata={"content-type": "custom"},
    )
    head = await store.get_async("stream", options={"head": True})
    assert head.meta["size"] == 6
    assert head.metadata == {"content-type": "custom"}
    assert head.properties == {"Content-Type": "application/octet-stream"}
    assert bytes(await (await store.get_async("stream")).bytes_async()) == b"onetwo"


def test_metadata_values_must_be_strings():
    store = MemoryStore()
    invalid_metadata: Any = {"bad": 123}
    with pytest.raises(TypeError):
        store.put("invalid", b"body", metadata=invalid_metadata)
    assert store.list().collect() == []
