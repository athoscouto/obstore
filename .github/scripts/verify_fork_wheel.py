"""Smoke-test the installed fork wheel without external services."""

from __future__ import annotations

import asyncio
import tomllib
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory

import obstore
from obstore.store import MemoryStore


async def main() -> None:
    """Exercise native Path uploads below and above the multipart threshold."""
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
