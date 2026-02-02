from datetime import datetime, timedelta, timezone
from typing import Any

from auth.token.models import Token
from auth.token.token_provider import TokenProvider


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
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
    )


def expired_token() -> Token:
    return Token(
        token_value="expired",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )


def almost_expired_token(seconds_until_expiry: int = 30) -> Token:
    """Token that expires soon - useful for testing refresh margin"""
    return Token(
        token_value="almost-expired",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=seconds_until_expiry),
    )


def token_with_custom_expiry(ttl_seconds: int) -> Token:
    """Token with custom TTL - useful for testing specific scenarios"""
    return Token(
        token_value=f"ttl-{ttl_seconds}",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
    )
