import pytest
from config.models.transport import (
    AiohttpEngineConfig,
    TcpConnectionConfig,
)
from request_execution.transport.base import TransportEngineType


@pytest.fixture
def aiohttp_config():
    return AiohttpEngineConfig(
        type=TransportEngineType.AIOHTTP,
        base_timeout=30,
        warmup_timeout=5,
        tcp_connection=TcpConnectionConfig(limit=50),
    )

