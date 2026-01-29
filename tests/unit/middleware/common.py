import pytest
from typing import Any
from auth import TokenManager
from request_execution import BearerTokenMiddleware, HeaderAuthMiddleware 
from tests.fixtures.request_execution import base_exchange, terminal_handler_ok


class FakeTokenManager(TokenManager):
    def __init__(self):
        self.provider = self

    async def get_token_value(self) -> str:
        return "fake-token"

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": "fake"}


@pytest.mark.asyncio
async def test_bearer_token_middleware_adds_header():
    mw = BearerTokenMiddleware(FakeTokenManager())

    req = base_exchange()
    result = await mw(req, terminal_handler_ok)

    assert result.context.headers["Authorization"] == "Bearer fake-token"
    assert result.metadata["token_provider"]["provider"] == "fake"


@pytest.mark.asyncio
async def test_header_auth_middleware_sets_basic_auth():
    mw = HeaderAuthMiddleware("user", "pass")

    req = base_exchange()
    result = await mw(req, terminal_handler_ok)

    auth = result.context.headers["Authorization"]
    assert auth.startswith("Basic ")

