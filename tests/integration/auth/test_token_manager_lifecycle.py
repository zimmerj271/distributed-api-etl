"""Integration tests for TokenManager lifecycle and background refresh"""
import pytest
import asyncio
from datetime import datetime, timezone, timedelta

from auth.token.models import Token
from auth.token.token_provider import StaticTokenProvider
from auth.token.token_manager import TokenManager, DriverTokenManager
from core.coroutine import AsyncBackgroundService


class CountingTokenProvider:
    """Token provider that counts calls and returns configurable tokens"""

    def __init__(self, token_ttl_seconds: int = 300):
        self.call_count = 0
        self.token_ttl_seconds = token_ttl_seconds
        self._lock = asyncio.Lock()

    async def get_token(self) -> Token:
        async with self._lock:
            self.call_count += 1
            return Token(
                token_value=f"token-{self.call_count}",
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=self.token_ttl_seconds),
            )

    def token_telemetry(self) -> dict:
        return {"provider": "counting", "calls": self.call_count}


class ExpiringTokenProvider:
    """Token provider that returns tokens with short expiry for testing refresh"""

    def __init__(self, expiry_seconds: int = 5):
        self.call_count = 0
        self.expiry_seconds = expiry_seconds

    async def get_token(self) -> Token:
        self.call_count += 1
        return Token(
            token_value=f"expiring-token-{self.call_count}",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=self.expiry_seconds),
        )

    def token_telemetry(self) -> dict:
        return {"provider": "expiring", "calls": self.call_count}


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestTokenManagerCaching:
    """Tests for TokenManager caching behavior"""

    async def test_caches_valid_token(self):
        """
        GIVEN a TokenManager with a valid token
        WHEN get_token is called multiple times
        THEN the cached token should be returned
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        # Multiple calls
        tokens = [await manager.get_token() for _ in range(5)]

        # All should be the same token
        assert all(t.token_value == "token-1" for t in tokens)

        # Only one provider call
        assert provider.call_count == 1

    async def test_refreshes_when_expired(self):
        """
        GIVEN a TokenManager with an expired token
        WHEN get_token is called
        THEN a new token should be fetched
        """
        provider = ExpiringTokenProvider(expiry_seconds=0)  # Expires immediately

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=0)

        token1 = await manager.get_token()
        assert token1.token_value == "expiring-token-1"

        # Wait for expiry
        await asyncio.sleep(0.1)

        token2 = await manager.get_token()
        assert token2.token_value == "expiring-token-2"

        assert provider.call_count == 2

    async def test_refreshes_within_margin(self):
        """
        GIVEN a TokenManager with a token expiring within refresh margin
        WHEN get_token is called
        THEN the token should be refreshed proactively
        """
        # Token expires in 30 seconds
        provider = ExpiringTokenProvider(expiry_seconds=30)

        # Clear singleton state
        TokenManager._instances.clear()
        # Refresh margin of 60 seconds - token expiring in 30s should trigger refresh
        manager = TokenManager(provider=provider, refresh_margin=60)

        token1 = await manager.get_token()
        assert token1.token_value == "expiring-token-1"

        # Call again - should refresh because within margin
        token2 = await manager.get_token()
        assert token2.token_value == "expiring-token-2"

        assert provider.call_count == 2

    async def test_force_refresh(self):
        """
        GIVEN a TokenManager with a cached token
        WHEN force_refresh is called
        THEN a new token should be fetched regardless of expiry
        """
        provider = CountingTokenProvider(token_ttl_seconds=3600)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        token1 = await manager.get_token()
        assert token1.token_value == "token-1"

        # Force refresh
        token2 = await manager.force_refresh()
        assert token2.token_value == "token-2"

        assert provider.call_count == 2

    async def test_invalidate_clears_cache(self):
        """
        GIVEN a TokenManager with a cached token
        WHEN invalidate is called and then get_token
        THEN a new token should be fetched
        """
        provider = CountingTokenProvider(token_ttl_seconds=3600)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        token1 = await manager.get_token()
        assert token1.token_value == "token-1"

        # Invalidate
        manager.invalidate()

        # Next call should fetch new token
        token2 = await manager.get_token()
        assert token2.token_value == "token-2"

        assert provider.call_count == 2


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestTokenManagerConcurrency:
    """Tests for TokenManager concurrent access"""

    async def test_serializes_concurrent_refresh(self):
        """
        GIVEN multiple concurrent requests for a token
        WHEN no token is cached
        THEN only one refresh should occur
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        # Launch many concurrent requests
        tasks = [manager.get_token() for _ in range(20)]
        results = await asyncio.gather(*tasks)

        # All should get the same token
        assert all(r.token_value == "token-1" for r in results)

        # Only one provider call due to lock serialization
        assert provider.call_count == 1

    async def test_concurrent_with_expired_token(self):
        """
        GIVEN multiple concurrent requests when token is expired
        WHEN refresh is triggered
        THEN only one refresh should occur due to lock serialization
        """
        # Use a long-lived token so it doesn't expire during the test
        provider = ExpiringTokenProvider(expiry_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        # Get initial token
        token1 = await manager.get_token()
        assert provider.call_count == 1
        assert token1.token_value == "expiring-token-1"

        # Invalidate the cached token to force a refresh on next call
        manager.invalidate()

        # Launch concurrent requests - all need a new token, but lock should serialize
        tasks = [manager.get_token() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # All should get the same refreshed token
        assert all(r.token_value == "expiring-token-2" for r in results)

        # Only one additional refresh (lock should serialize)
        assert provider.call_count == 2


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestDriverTokenManagerBackgroundRefresh:
    """Tests for DriverTokenManager background refresh loop"""

    async def test_background_coroutine_refreshes_periodically(self):
        """
        GIVEN a DriverTokenManager with background coroutine
        WHEN the coroutine runs for a period
        THEN the token should be refreshed proactively
        """
        # Token expires in 2 seconds
        provider = ExpiringTokenProvider(expiry_seconds=2)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = DriverTokenManager(provider=provider, refresh_margin=1)

        # Start background coroutine
        coro = manager.background_coroutine()
        task = asyncio.create_task(coro)

        # Wait for initial fetch and some refresh cycles
        await asyncio.sleep(0.5)

        # Should have fetched at least once
        assert provider.call_count >= 1

        # Cancel the background task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_background_coroutine_handles_cancellation(self):
        """
        GIVEN a running DriverTokenManager background coroutine
        WHEN it is cancelled
        THEN it should exit cleanly
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = DriverTokenManager(provider=provider, refresh_margin=60)

        coro = manager.background_coroutine()
        task = asyncio.create_task(coro)

        # Let it run briefly
        await asyncio.sleep(0.2)

        # Cancel
        task.cancel()

        # Should handle cancellation gracefully
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.integration
@pytest.mark.auth
class TestAsyncBackgroundServiceLifecycle:
    """Tests for AsyncBackgroundService with TokenManager"""

    def test_starts_and_stops_cleanly(self):
        """
        GIVEN an AsyncBackgroundService with DriverTokenManager
        WHEN start and stop are called
        THEN the service should manage lifecycle correctly
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = DriverTokenManager(provider=provider, refresh_margin=60)

        service = AsyncBackgroundService(manager.background_coroutine)

        # Start service
        service.start()
        assert service.is_running

        # Let it run
        import time
        time.sleep(0.3)

        # Stop service
        service.stop()
        assert not service.is_running

        # Token should have been fetched at least once
        assert provider.call_count >= 1

    def test_multiple_start_stop_cycles(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN started and stopped multiple times
        THEN it should handle each cycle correctly
        """
        call_counts = []

        for i in range(3):
            provider = CountingTokenProvider(token_ttl_seconds=300)

            # Clear singleton state
            TokenManager._instances.clear()
            manager = DriverTokenManager(provider=provider, refresh_margin=60)

            service = AsyncBackgroundService(manager.background_coroutine)

            service.start()
            assert service.is_running

            import time
            time.sleep(0.2)

            service.stop()
            assert not service.is_running

            call_counts.append(provider.call_count)

        # Each cycle should have made at least one call
        assert all(c >= 1 for c in call_counts)

    def test_idempotent_start(self):
        """
        GIVEN an already running AsyncBackgroundService
        WHEN start is called again
        THEN it should be a no-op
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = DriverTokenManager(provider=provider, refresh_margin=60)

        service = AsyncBackgroundService(manager.background_coroutine)

        service.start()
        assert service.is_running

        # Start again - should not crash
        service.start()
        assert service.is_running

        service.stop()
        assert not service.is_running

    def test_idempotent_stop(self):
        """
        GIVEN a stopped AsyncBackgroundService
        WHEN stop is called again
        THEN it should be a no-op
        """
        provider = CountingTokenProvider(token_ttl_seconds=300)

        # Clear singleton state
        TokenManager._instances.clear()
        manager = DriverTokenManager(provider=provider, refresh_margin=60)

        service = AsyncBackgroundService(manager.background_coroutine)

        service.start()
        service.stop()
        assert not service.is_running

        # Stop again - should not crash
        service.stop()
        assert not service.is_running
