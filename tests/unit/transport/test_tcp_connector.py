import pytest
from aiohttp import TCPConnector
from request_execution.transport.engine import AiohttpEngine
from tests.fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.asyncio
async def test_build_tcp_connector_without_tls():
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    async with engine:
        connector = engine._connector
        assert isinstance(connector, TCPConnector)
