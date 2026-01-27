import pytest
from unittest.mock import AsyncMock

from transport.engine import AiohttpEngine
from transport.base import TransportRequest
from tests.fixtures.transport import tcp_config_no_tls


@pytest.mark.asyncio
async def test_send_handles_exception():
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    # fake session
    mock_session = AsyncMock()
    mock_session.request.side_effect = RuntimeError("boom")
    engine.session = mock_session

    req = TransportRequest(
        method="GET",
        url="test",
        headers={},
    )

    resp = await engine.send(req)

    assert resp.status is None
    assert resp.error is not None
    assert "RuntimeError" in resp.error

