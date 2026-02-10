import asyncio
from datetime import datetime, timedelta
from auth.token.token_provider import Token
from auth.token.token_manager import TokenManager


class FakeTokenManager(TokenManager):
    def __init__(self, fail: bool = False):
        self.fail = fail

    async def get_token(self) -> Token:
        if self.fail:
            raise RuntimeError("token failure")
        return Token(
            token_value="rpc-token",
            expires_at=datetime.now() + timedelta(seconds=60),
        )

    def background_coroutine(self):
        async def noop():
            while True:
                await asyncio.sleep(3600)
        return noop

