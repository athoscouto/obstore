import pytest

from tests.streaming_https import run_smoke


@pytest.mark.asyncio
async def test_put_async_path_streams_through_public_api():
    await run_smoke()


@pytest.mark.asyncio
async def test_single_part_paths_above_32_mib_still_stream():
    await run_smoke(large_size=33 * 1024 * 1024 + 1)
