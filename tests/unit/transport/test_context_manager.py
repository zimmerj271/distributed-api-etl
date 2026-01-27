import pytest
from transport.engine import AiohttpEngine
from tests.fixtures.transport import tcp_config_no_tls


@pytest.mark.asyncio
async def test_aiohttp_engine_context_manager():
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    async with engine:
        assert engine.session is not None
        assert engine.session.closed is False

    # after exit
    assert engine._session is None

