"""Test doubles for RPC services"""
import asyncio
from datetime import datetime, timedelta
from auth.token.models import Token
from auth.token.token_manager import TokenManager


class FakeTokenManager(TokenManager):
    """
    Fake token manager for testing RPC services

    Args:
        fail: If True, get_token() will raise an error
    """
    def __init__(self, fail: bool = False):
        self.fail = fail
        self._call_count = 0

    async def get_token(self) -> Token:
        """Return a fake token or raise if fail=True"""
        self._call_count += 1

        if self.fail:
            raise RuntimeError("token failure")

        return Token(
            token_value="fake-rpc-token",
            expires_at=datetime.now() + timedelta(seconds=60),
        )

    def background_coroutine(self):
        """Return a no-op coroutine"""
        async def noop():
            while True:
                await asyncio.sleep(3600)
        return noop

    @property
    def call_count(self) -> int:
        """Number of times get_token was called"""
        return self._call_count

