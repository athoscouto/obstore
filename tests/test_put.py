from __future__ import annotations

import asyncio
import itertools
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import obstore
from obstore.exceptions import AlreadyExistsError
from obstore.store import LocalStore, MemoryStore, S3Store


def test_put_non_multipart():
    store = MemoryStore()

    store.put("file1.txt", b"foo", use_multipart=False)
    assert store.get("file1.txt").bytes() == b"foo"


def test_put_path_sync(tmp_path: Path):
    store = MemoryStore()
    source = tmp_path / "source.txt"
    source.write_bytes(b"file input")

    store.put("file1.txt", source, use_multipart=False)

    assert store.get("file1.txt").bytes() == b"file input"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_multipart", [None, False, True])
async def test_put_async_path(*, use_multipart: bool | None, tmp_path: Path):
    store = MemoryStore()
    source = tmp_path / "source.txt"
    source.write_bytes(b"file input with multipart boundary")

    if use_multipart is None:
        await store.put_async("file1.txt", source, chunk_size=8)
    else:
        await store.put_async(
            "file1.txt",
            source,
            chunk_size=8,
            use_multipart=use_multipart,
        )

    assert store.get("file1.txt").bytes() == b"file input with multipart boundary"


@pytest.mark.asyncio
async def test_put_async_missing_path_raises_when_awaited(tmp_path: Path):
    store = MemoryStore()
    missing = tmp_path / "missing.txt"

    pending_put = obstore.put_async(store, "missing.txt", missing, use_multipart=False)

    with pytest.raises(FileNotFoundError):
        await pending_put


@pytest.mark.asyncio
@pytest.mark.parametrize("use_multipart", [None, True])
async def test_put_async_path_create_mode_forces_single_put(
    *,
    use_multipart: bool | None,
    tmp_path: Path,
):
    store = MemoryStore()
    source = tmp_path / "source.txt"
    source.write_bytes(b"replacement")
    store.put("file1.txt", b"original")

    if use_multipart is None:
        pending_put = obstore.put_async(
            store,
            "file1.txt",
            source,
            mode="create",
            chunk_size=4,
        )
    else:
        pending_put = obstore.put_async(
            store,
            "file1.txt",
            source,
            mode="create",
            use_multipart=use_multipart,
            chunk_size=4,
        )

    with pytest.raises(AlreadyExistsError):
        await pending_put
    assert store.get("file1.txt").bytes() == b"original"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
async def test_path_put_uses_multipart_above_default_chunk_size(
    *,
    minio_store: S3Store,
    tmp_path: Path,
    use_async: bool,
):
    chunk_size = 5 * 1024 * 1024

    for size, is_multipart in ((chunk_size, False), (chunk_size + 1, True)):
        source = tmp_path / f"source-{size}.bin"
        source.write_bytes(b"x" * size)
        key = f"{'async' if use_async else 'sync'}-{size}.bin"

        if use_async:
            result = await obstore.put_async(minio_store, key, source)
        else:
            result = obstore.put(minio_store, key, source)

        e_tag = result["e_tag"]
        assert e_tag is not None
        assert e_tag.strip('"').endswith("-2") is is_multipart


def _start_delayed_fifo_writer(path: Path, delay: float) -> subprocess.Popen[bytes]:
    writer_script = """
import os
import sys
import time

time.sleep(float(sys.argv[2]))
fd = os.open(sys.argv[1], os.O_WRONLY)
try:
    os.write(fd, b"fifo payload")
except BrokenPipeError:
    pass
finally:
    os.close(fd)
"""
    # Only the temporary FIFO path is dynamic; the interpreter and script are fixed.
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", writer_script, str(path), str(delay)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_delayed_fifo_data_writer(path: Path, delay: float) -> subprocess.Popen[bytes]:
    writer_script = """
import os
import sys
import time

fd = os.open(sys.argv[1], os.O_WRONLY)
try:
    time.sleep(float(sys.argv[2]))
    os.write(fd, b"fifo payload")
except BrokenPipeError:
    pass
finally:
    os.close(fd)
"""
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", writer_script, str(path), str(delay)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _has_open_file_descriptor_for(path: Path) -> bool:
    path_stat = path.stat()
    for descriptor in Path("/dev/fd").iterdir():
        try:
            opened_stat = os.fstat(int(descriptor.name))
        except (OSError, ValueError):
            continue
        if (opened_stat.st_dev, opened_stat.st_ino) == (
            path_stat.st_dev,
            path_stat.st_ino,
        ):
            return True
    return False


def _wait_for_fifo_writer(writer: subprocess.Popen[bytes]) -> int:
    try:
        return writer.wait(timeout=5)
    except subprocess.TimeoutExpired:
        writer.kill()
        return writer.wait(timeout=5)


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires named pipes")
@pytest.mark.parametrize("api", ["async", "sync_thread"])
async def test_put_path_open_does_not_block_event_loop(tmp_path: Path, api: str):
    fifo = tmp_path / "source.fifo"
    os.mkfifo(fifo)
    writer = _start_delayed_fifo_writer(fifo, 0.25)
    store = MemoryStore()
    heartbeats = 0

    async def heartbeat() -> None:
        nonlocal heartbeats
        while True:
            heartbeats += 1
            await asyncio.sleep(0.005)

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await asyncio.sleep(0.02)
        initial_heartbeats = heartbeats

        if api == "async":
            await obstore.put_async(store, "fifo.txt", fifo, use_multipart=False)
        else:
            await asyncio.to_thread(
                obstore.put,
                store,
                "fifo.txt",
                fifo,
                use_multipart=False,
            )

        assert heartbeats - initial_heartbeats >= 10
        assert store.get("fifo.txt").bytes() == b"fifo payload"
        assert await asyncio.to_thread(_wait_for_fifo_writer, writer) == 0
    finally:
        heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_task
        await asyncio.to_thread(_wait_for_fifo_writer, writer)


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires named pipes")
@pytest.mark.parametrize("use_multipart", [False, True])
async def test_put_async_path_read_does_not_block_event_loop(
    tmp_path: Path,
    use_multipart: bool,
):
    fifo = tmp_path / "source.fifo"
    os.mkfifo(fifo)
    writer = _start_delayed_fifo_data_writer(fifo, 0.25)
    store = MemoryStore()
    heartbeats = 0

    async def heartbeat() -> None:
        nonlocal heartbeats
        while True:
            heartbeats += 1
            await asyncio.sleep(0.005)

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await asyncio.sleep(0.02)
        initial_heartbeats = heartbeats
        await obstore.put_async(
            store,
            "fifo.txt",
            fifo,
            use_multipart=use_multipart,
            chunk_size=4,
        )

        assert heartbeats - initial_heartbeats >= 10
        assert store.get("fifo.txt").bytes() == b"fifo payload"
        assert await asyncio.to_thread(_wait_for_fifo_writer, writer) == 0
    finally:
        heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_task
        await asyncio.to_thread(_wait_for_fifo_writer, writer)


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires named pipes")
async def test_put_async_cancelled_while_reading_path_closes_file(tmp_path: Path):
    fifo = tmp_path / "source.fifo"
    os.mkfifo(fifo)
    writer = _start_delayed_fifo_data_writer(fifo, 0.25)
    store = MemoryStore()

    async def put_fifo() -> None:
        await obstore.put_async(store, "fifo.txt", fifo, use_multipart=False)

    put_task = asyncio.create_task(put_fifo())
    try:
        await asyncio.sleep(0.05)
        put_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await put_task

        assert await asyncio.to_thread(_wait_for_fifo_writer, writer) == 0
        for _ in range(100):
            if not await asyncio.to_thread(_has_open_file_descriptor_for, fifo):
                break
            await asyncio.sleep(0.01)
        assert not await asyncio.to_thread(_has_open_file_descriptor_for, fifo)
        assert store.list().collect() == []
    finally:
        if not put_task.done():
            put_task.cancel()
        await asyncio.to_thread(_wait_for_fifo_writer, writer)


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires named pipes")
async def test_put_async_cancelled_while_opening_path_does_not_upload(
    tmp_path: Path,
):
    fifo = tmp_path / "source.fifo"
    os.mkfifo(fifo)
    writer = _start_delayed_fifo_writer(fifo, 0.25)
    store = MemoryStore()

    async def put_fifo() -> None:
        await obstore.put_async(store, "fifo.txt", fifo, use_multipart=False)

    put_task = asyncio.create_task(put_fifo())
    try:
        await asyncio.sleep(0.05)
        put_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await put_task

        assert await asyncio.to_thread(_wait_for_fifo_writer, writer) == 0
        deadline = asyncio.get_running_loop().time() + 2
        # Poll the native descriptor table; there is no event to subscribe to.
        while (  # noqa: ASYNC110
            _has_open_file_descriptor_for(fifo)
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.01)
        assert not _has_open_file_descriptor_for(fifo)

        await asyncio.sleep(0.05)
        with pytest.raises(FileNotFoundError):
            store.get("fifo.txt")
    finally:
        if put_task.done() is False:
            put_task.cancel()
        await asyncio.to_thread(_wait_for_fifo_writer, writer)


def test_put_non_multipart_sync_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"
    iterator = itertools.repeat(b, 5)
    store.put("file1.txt", iterator, use_multipart=False)
    assert store.get("file1.txt").bytes() == (b * 5)


@pytest.mark.asyncio
async def test_put_non_multipart_async_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"

    async def it():
        for _ in range(5):
            yield b"the quick brown fox jumps over the lazy dog,"

    await store.put_async("file1.txt", it(), use_multipart=False)
    assert store.get("file1.txt").bytes() == (b * 5)


def test_put_multipart_one_chunk():
    store = MemoryStore()

    store.put("file1.txt", b"foo", use_multipart=True)
    assert store.get("file1.txt").bytes() == b"foo"


def test_put_multipart_large():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 5000
    path = "big-data.txt"

    store.put(path, data, use_multipart=True)
    assert store.get(path).bytes() == data


def test_put_mode():
    store = MemoryStore()

    store.put("file1.txt", b"foo")
    store.put("file1.txt", b"bar", mode="overwrite")

    with pytest.raises(AlreadyExistsError):
        store.put("file1.txt", b"foo", mode="create")

    assert store.get("file1.txt").bytes() == b"bar"


@pytest.mark.asyncio
async def test_put_async_iterable():
    store = MemoryStore()

    data = b"the quick brown fox jumps over the lazy dog," * 50_000
    path = "big-data.txt"

    await store.put_async(path, data)

    resp = await store.get_async(path)
    stream = resp.stream(min_chunk_size=0)
    new_path = "new-path.txt"
    await store.put_async(new_path, stream)

    assert store.get(new_path).bytes() == data


def test_put_sync_iterable():
    store = MemoryStore()

    b = b"the quick brown fox jumps over the lazy dog,"
    iterator = itertools.repeat(b, 50_000)
    data = b * 50_000
    path = "big-data.txt"

    store.put(path, iterator)

    assert store.get(path).bytes() == data


def test_put_sync_iterable_local_store():
    """Issue #450."""
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)

        b = b"the quick brown fox jumps over the lazy dog,"
        iterator = itertools.repeat(b, 50_000)
        data = b * 50_000
        path = "big-data.txt"

        store.put(path, iterator)

        assert store.get(path).bytes() == data


CHUNK_SIZE = 1024


@pytest.mark.parametrize("nbytes", [0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1])
def test_put_multipart_buffer_chunk_boundaries(nbytes: int):
    """Buffers are chunked correctly regardless of alignment to chunk_size."""
    store = MemoryStore()
    data = bytes(range(256)) * (nbytes // 256 + 1)
    data = data[:nbytes]

    store.put("file1.txt", data, use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == data


@pytest.mark.parametrize("wrapper", [bytes, bytearray, memoryview])
def test_put_multipart_buffer_types(
    wrapper: type[bytes | bytearray | memoryview],
):
    """Any buffer-protocol input is uploaded verbatim."""
    store = MemoryStore()
    data = b"the quick brown fox jumps over the lazy dog," * 500

    store.put("file1.txt", wrapper(data), use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == data


def test_put_multipart_buffer_slice():
    """A memoryview into the middle of a larger buffer uploads only its own bytes."""
    store = MemoryStore()
    backing = bytes(range(256)) * 40
    view = memoryview(backing)[1000:8000]

    store.put("file1.txt", view, use_multipart=True, chunk_size=CHUNK_SIZE)

    assert store.get("file1.txt").bytes() == backing[1000:8000]


def test_put_multipart_buffer_local_store():
    """The buffer multipart path round-trips through a store with real parts."""
    with TemporaryDirectory() as tmpdir:
        store = LocalStore(tmpdir)
        data = b"the quick brown fox jumps over the lazy dog," * 500

        store.put("big-data.txt", data, use_multipart=True, chunk_size=CHUNK_SIZE)

        assert store.get("big-data.txt").bytes() == data
