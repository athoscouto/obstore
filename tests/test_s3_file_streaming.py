import pytest

from tests.streaming_https import run_smoke


@pytest.mark.asyncio
async def test_put_async_path_streams_through_public_api():
    await run_smoke()
