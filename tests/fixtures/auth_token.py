from datetime import datetime, timedelta
from typing import Any

from auth.token.token_provider import Token, TokenProvider


class FakeTokenProvider(TokenProvider):
    def __init__(self, token: Token):
        self._token = token
        self.calls = 0

    async def get_token(self) -> Token:
        self.calls += 1
        return self._token

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": "fake"}


class FailingTokenProvider(TokenProvider):
    async def get_token(self) -> Token:
        raise RuntimeError("provider failed")

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": "fail"}


def valid_token(ttl_seconds: int = 300) -> Token:
    return Token(
        token_value="abc123",
        expires_at=datetime.now() + timedelta(seconds=ttl_seconds),
    )


def expired_token() -> Token:
    return Token(
        token_value="expired",
        expires_at=datetime.now() - timedelta(seconds=10),
    )

