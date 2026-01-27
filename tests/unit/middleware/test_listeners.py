import pytest
from middleware.listeners import LoggingMiddleware, TimingMiddleware, WorkerIdentityMiddleware
from tests.fixtures.middleware import base_exchange, terminal_handler_ok


@pytest.mark.asyncio
async def test_logging_middleware_records_logs():
    mw = LoggingMiddleware()
    req = base_exchange()

    result = await mw(req, terminal_handler_ok)

    logs = result.metadata["logs"]
    assert logs[0].startswith("->")
    assert logs[1].startswith("<-")


@pytest.mark.asyncio
async def test_timing_middleware_sets_duration():
    mw = TimingMiddleware()
    req = base_exchange()

    result = await mw(req, terminal_handler_ok)

    timing = result.metadata["timing"]
    assert "total_seconds" in timing
    assert timing["total_seconds"] >= 0.0


@pytest.mark.asyncio
async def test_worker_identity_middleware_sets_metadata():
    mw = WorkerIdentityMiddleware()
    req = base_exchange()

    result = await mw(req, terminal_handler_ok)

    identity = result.metadata["executor_identity"]
    assert "hostname" in identity
    assert "pid" in identity

