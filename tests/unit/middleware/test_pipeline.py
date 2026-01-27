import pytest
from middleware.pipeline import MiddlewarePipeline
from tests.fixtures.middleware import (
    base_exchange,
    terminal_handler_ok,
)


@pytest.mark.asyncio
async def test_pipeline_executes_in_order():
    calls: list[str] = []

    async def mw1(req, next_call):
        calls.append("mw1")
        return await next_call(req)

    async def mw2(req, next_call):
        calls.append("mw2")
        return await next_call(req)

    pipeline = MiddlewarePipeline()
    pipeline.add(mw1)
    pipeline.add(mw2)

    await pipeline.execute(base_exchange(), terminal_handler_ok)

    assert calls == ["mw1", "mw2"]


@pytest.mark.asyncio
async def test_pipeline_reaches_terminal_handler():
    pipeline = MiddlewarePipeline()
    result = await pipeline.execute(base_exchange(), terminal_handler_ok)

    assert result.status_code == 200

