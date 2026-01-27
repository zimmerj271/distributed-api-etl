import logging
import asyncio
from typing import Coroutine, Any
from core.singleton import SingletonMeta
from auth.token.token_provider import TokenProvider, Token


class TokenManager(metaclass=SingletonMeta):
    """
    Asynchronous token manager responsible for acquiring and caching a single
    OAuth2 access token from a given TokenProvider.

    TokenManager is designed to be usable in both driver and worker contexts:
      • On the driver, it is typically extended by DriverTokenManager and ran 
        on background refresh loop using AsyncBackgroundService.
      • On workers, it can be used directly behind an RpcTokenProvider to
        interpret token responses returned from the driver’s RPC token service.

    Core responsibilities:
      • Hold the current Token instance.
      • Lazily refresh the token when it is missing, expired, or within a
        configured refresh margin.
      • Serialize concurrent refresh attempts using an asyncio.Lock so multiple
        callers don’t all trigger separate token requests.
      • Provide convenience methods for getting the Token or just its string
        value, and for forcing a refresh or invalidating the cached token.

    TokenManager itself does **not** own any event loop or threads; it simply
    exposes async methods that are called inline from existing async code 
    (e.g., middleware or async partition processing), using the node’s own event loop.
    """

    def __init__(self, provider: TokenProvider, refresh_margin: int = 60) -> None:
        self.provider = provider
        self._refresh_margin = refresh_margin
        self._token: Token | None = None
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(f"[{self.__class__.__name__}]")

    async def _refresh_token(self) -> Token:

        # Fast path - token exists and is not near expiring.
        if (
            self._token is not None
            and not self._token.is_expired
            and not self._token.will_expire_within(self._refresh_margin)
        ):
            return self._token

        async with self._lock:
            if (
                self._token is None
                or self._token.is_expired
                or self._token.will_expire_within(self._refresh_margin)
            ):
                self._token = await self.provider.get_token()

        return self._token

    async def get_token(self) -> Token:
        return await self._refresh_token()

    async def get_token_value(self) -> str:
        token = await self._refresh_token()
        return token.token_value

    async def force_refresh(self) -> Token:
        async with self._lock:
            self._token = await self.provider.get_token()
            return self._token

    def invalidate(self) -> None:
        self._token = None


class DriverTokenManager(TokenManager):
    """
    Driver-specialized TokenManager that provides a long-running background
    coroutine to keep the token fresh for the lifetime of the job.

    While TokenManager defines the core logic for acquiring and refreshing
    tokens, DriverTokenManager extends TokenManager and also implements a 
    the structural interface BackgroundProcess (core.coroutine) by 
    implementing a `background_coroutine` that can be scheduled and run in 
    a dedicated event loop (for example via AsyncBackgroundService on the driver).

    Behavior:
    • `background_coroutine()` returns an async function that:
        - Performs an initial token acquisition.
        - Logs that the background refresh loop has started.
        - Periodically wakes up (e.g., once per second) and calls
          `get_token()` so the token is refreshed before expiration.
    • The loop runs until it receives an asyncio.CancelledError, at which
        point it logs shutdown and exits cleanly.

    Typical usage in the ETL pipeline:
    • On the driver, a DriverTokenManager is constructed with a
        PasswordGrantTokenProvider.
    • AsyncBackgroundService is created with
        `background_fn=driver_token_manager.background_coroutine`.
    • AsyncBackgroundService.start() runs the loop in a dedicated thread and
        event loop, keeping the token valid while the rest of the pipeline
        executes.
    • When the pipeline completes, AsyncBackgroundService.stop() cancels the
        loop and shuts down the event loop/thread.

    This pattern ensures that all distributed worker nodes can safely rely on a
    single, continuously refreshed token managed by the driver.
    """

    def background_coroutine(self) -> Coroutine[Any, Any, None]:
        """
        Implementation of the BackgroundProcess structural interface and
        wraps an async function which refreshes the token in a background loop.
        """

        async def _refresh_token_loop() -> None:
            # initial refresh
            await self.get_token()
            self._logger.info(
                "Background token refresh loop started "
                f"(margin={self._refresh_margin}s)"
            )

            try:
                # Loop indefinitely until BackgroundProcess is ended
                while True:
                    await asyncio.sleep(1)
                    await self.get_token()

            except asyncio.CancelledError:
                self._logger.info("Background refresh loop cancelled")
                raise

        return _refresh_token_loop()
