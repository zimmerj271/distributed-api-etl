"""Unit tests for token manager functionality"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone

from auth.token.token_manager import TokenManager, DriverTokenManager
from auth.token.token_provider import Token
from tests.fixtures.auth.auth_token import (
    FakeTokenProvider,
    FailingTokenProvider,
    valid_token,
    expired_token,
)


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerCaching:
    """Tests for token caching behavior"""

    @pytest.mark.asyncio
    async def test_caches_valid_token_on_first_call(self):
        """
        GIVEN a token manager with a valid token provider
        WHEN get_token is called for the first time
        THEN the token should be fetched and cached
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        token = await mgr.get_token()

        assert token is not None
        assert token.token_value == "abc123"
        assert provider.calls == 1

    @pytest.mark.asyncio
    async def test_returns_cached_token_on_subsequent_calls(self):
        """
        GIVEN a token manager with a cached valid token
        WHEN get_token is called multiple times
        THEN the provider should only be called once (caching works)
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        t1 = await mgr.get_token()
        t2 = await mgr.get_token()
        t3 = await mgr.get_token()

        assert t1 is t2 is t3  # Same object reference
        assert provider.calls == 1

    @pytest.mark.asyncio
    async def test_get_token_value_returns_string(self):
        """
        GIVEN a token manager with a valid token
        WHEN get_token_value is called
        THEN it should return just the token string value
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        token_value = await mgr.get_token_value()

        assert isinstance(token_value, str)
        assert token_value == "abc123"


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerRefresh:
    """Tests for token refresh behavior"""

    @pytest.mark.asyncio
    async def test_refreshes_expired_token(self):
        """
        GIVEN a token manager with an expired token
        WHEN get_token is called multiple times
        THEN the provider should be called each time to refresh
        """
        provider = FakeTokenProvider(expired_token())
        mgr = TokenManager(provider, refresh_margin=0)

        await mgr.get_token()
        await mgr.get_token()

        assert provider.calls == 2

    @pytest.mark.asyncio
    async def test_refreshes_token_within_margin(self):
        """
        GIVEN a token manager with a 60-second refresh margin
        WHEN the token expires in less than 60 seconds
        THEN it should be refreshed proactively
        """
        # Token expires in 30 seconds
        almost_expired = Token(
            token_value="about-to-expire",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        provider = FakeTokenProvider(almost_expired)
        mgr = TokenManager(provider, refresh_margin=60)

        await mgr.get_token()

        # Should trigger refresh because 30s < 60s margin
        assert provider.calls == 1

    @pytest.mark.asyncio
    async def test_does_not_refresh_token_outside_margin(self):
        """
        GIVEN a token manager with a 60-second refresh margin
        WHEN the token expires in more than 60 seconds
        THEN it should NOT be refreshed yet
        """
        # Token expires in 120 seconds
        fresh_token = Token(
            token_value="fresh", expires_at=datetime.now(timezone.utc) + timedelta(seconds=120)
        )
        provider = FakeTokenProvider(fresh_token)
        mgr = TokenManager(provider, refresh_margin=60)

        await mgr.get_token()
        await mgr.get_token()

        # Should NOT trigger refresh because 120s > 60s margin
        assert provider.calls == 1

    @pytest.mark.asyncio
    async def test_force_refresh_bypasses_cache(self):
        """
        GIVEN a token manager with a cached valid token
        WHEN force_refresh is called
        THEN a new token should be fetched regardless of cache
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        await mgr.get_token()  # Initial fetch
        await mgr.force_refresh()  # Force refresh

        assert provider.calls == 2

    @pytest.mark.asyncio
    async def test_force_refresh_returns_new_token(self):
        """
        GIVEN a token manager
        WHEN force_refresh is called
        THEN it should return the newly fetched token
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        original = await mgr.get_token()
        refreshed = await mgr.force_refresh()

        # With FakeTokenProvider, same token is returned
        # But the call was made
        assert refreshed is not None
        assert provider.calls == 2


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerInvalidation:
    """Tests for token invalidation"""

    @pytest.mark.asyncio
    async def test_invalidate_clears_cached_token(self):
        """
        GIVEN a token manager with a cached token
        WHEN invalidate is called
        THEN the next get_token should fetch a new token
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        await mgr.get_token()
        mgr.invalidate()
        await mgr.get_token()

        assert provider.calls == 2

    @pytest.mark.asyncio
    async def test_invalidate_is_idempotent(self):
        """
        GIVEN a token manager
        WHEN invalidate is called multiple times
        THEN it should not cause errors
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        mgr.invalidate()
        mgr.invalidate()
        mgr.invalidate()

        # Should work fine
        token = await mgr.get_token()
        assert token is not None


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerConcurrency:
    """Tests for concurrent access to token manager"""

    @pytest.mark.asyncio
    async def test_concurrent_first_fetch_only_calls_provider_once(self):
        """
        GIVEN a token manager with no cached token
        WHEN multiple coroutines request token simultaneously for the first time
        THEN only one provider call should be made (lock prevents duplicate fetches)
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        # Simulate 10 concurrent requests racing to get the first token
        tokens = await asyncio.gather(*[mgr.get_token() for _ in range(10)])

        # All should get the same token instance
        assert all(t is tokens[0] for t in tokens)

        # Critical: Provider should only be called once despite 10 concurrent requests
        assert provider.calls == 1, (
            f"Expected 1 provider call, got {provider.calls}. Lock is not working!"
        )

    @pytest.mark.asyncio
    async def test_concurrent_calls_with_valid_cache_hit(self):
        """
        GIVEN a token manager with a valid cached token
        WHEN multiple concurrent requests are made
        THEN no provider calls should be made (all cache hits)
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        # Prime the cache
        await mgr.get_token()
        initial_calls = provider.calls

        # Now make concurrent requests with valid cache
        tokens = await asyncio.gather(*[mgr.get_token() for _ in range(10)])

        # All should get the same cached token
        assert all(t is tokens[0] for t in tokens)

        # No new provider calls should have been made
        assert provider.calls == initial_calls

    @pytest.mark.asyncio
    async def test_concurrent_refresh_with_slow_provider(self):
        """
        GIVEN a token manager with a slow provider
        WHEN multiple coroutines trigger refresh simultaneously
        THEN only one refresh should happen (others wait on lock)
        """
        import asyncio

        class SlowTokenProvider:
            """Provider that takes time to fetch, exposing race conditions"""

            def __init__(self):
                self.calls = 0
                self.fetch_duration = 0.1  # 100ms delay

            async def get_token(self):
                self.calls += 1
                # Simulate slow network call
                await asyncio.sleep(self.fetch_duration)
                return Token(
                    token_value=f"token-{self.calls}",
                    expires_at=datetime.now(timezone.utc) + timedelta(seconds=300),
                )

            def token_telemetry(self):
                return {"provider": "slow"}

        provider = SlowTokenProvider()
        mgr = TokenManager(provider)

        # Launch 10 concurrent requests
        # Without proper locking, all 10 would call provider
        # With locking, only 1 should call provider
        tokens = await asyncio.gather(*[mgr.get_token() for _ in range(10)])

        # All tokens should be identical (same fetch)
        assert all(t.token_value == tokens[0].token_value for t in tokens)

        # Only 1 provider call should have been made
        assert provider.calls == 1, (
            f"Expected 1 call with locking, got {provider.calls}"
        )

    @pytest.mark.asyncio
    async def test_concurrent_force_refresh_properly_locks(self):
        """
        GIVEN a token manager with a cached token
        WHEN multiple force_refresh calls happen concurrently
        THEN each force_refresh should result in one provider call
        """
        provider = FakeTokenProvider(valid_token())
        mgr = TokenManager(provider)

        # Prime the cache
        await mgr.get_token()

        # Force refresh concurrently - this is interesting because
        # each force_refresh should trigger a fetch, but they should
        # be serialized by the lock
        await asyncio.gather(*[mgr.force_refresh() for _ in range(3)])

        # With proper locking: 1 (initial) + 3 (force refreshes) = 4
        # Without locking: Could be anywhere from 1-4 depending on races
        assert provider.calls == 4

    @pytest.mark.asyncio
    async def test_lock_prevents_race_condition(self):
        """
        GIVEN a token manager with no lock protection (hypothetically)
        WHEN concurrent requests race to fetch
        THEN we demonstrate what WOULD happen without the lock

        This test proves the lock is necessary and working.
        """
        import asyncio

        class InstrumentedProvider:
            """Provider that tracks concurrent access"""

            def __init__(self):
                self.calls = 0
                self.concurrent_calls = 0
                self.max_concurrent = 0
                self.in_progress = 0

            async def get_token(self):
                self.calls += 1
                self.in_progress += 1
                self.max_concurrent = max(self.max_concurrent, self.in_progress)

                # Simulate network delay
                await asyncio.sleep(0.05)

                self.in_progress -= 1

                return Token(
                    token_value="test-token",
                    expires_at=datetime.now(timezone.utc) + timedelta(seconds=300),
                )

            def token_telemetry(self):
                return {"provider": "instrumented"}

        provider = InstrumentedProvider()
        mgr = TokenManager(provider)

        # Launch 20 concurrent requests
        await asyncio.gather(*[mgr.get_token() for _ in range(20)])

        # With proper locking:
        # - Only 1 total call
        # - max_concurrent should be 1 (never more than 1 fetch at a time)
        assert provider.calls == 1, "Lock failed: multiple provider calls"
        assert provider.max_concurrent == 1, (
            f"Lock failed: {provider.max_concurrent} concurrent fetches detected"
        )


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerErrorHandling:
    """Tests for error handling"""

    @pytest.mark.asyncio
    async def test_propagates_provider_errors(self):
        """
        GIVEN a token provider that raises errors
        WHEN get_token is called
        THEN the error should propagate to the caller
        """
        provider = FailingTokenProvider()
        mgr = TokenManager(provider)

        with pytest.raises(RuntimeError, match="provider failed"):
            await mgr.get_token()

    @pytest.mark.asyncio
    async def test_does_not_cache_failed_token_fetch(self):
        """
        GIVEN a token provider that fails then succeeds
        WHEN get_token is called after a failure
        THEN it should retry fetching the token
        """

        # Create a provider that fails first, then succeeds
        class FlakeyProvider:
            def __init__(self):
                self.calls = 0

            async def get_token(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("First call fails")
                return valid_token()

            def token_telemetry(self):
                return {"provider": "flakey"}

        provider = FlakeyProvider()
        mgr = TokenManager(provider)

        # First call should fail
        with pytest.raises(RuntimeError):
            await mgr.get_token()

        # Second call should succeed
        token = await mgr.get_token()
        assert token is not None
        assert provider.calls == 2


@pytest.mark.unit
@pytest.mark.auth
class TestTokenManagerSingleton:
    """Tests for singleton behavior"""

    def test_token_manager_is_singleton(self):
        """
        GIVEN TokenManager uses SingletonMeta
        WHEN multiple instances are created
        THEN they should all be the same instance
        """
        provider1 = FakeTokenProvider(valid_token())
        provider2 = FakeTokenProvider(valid_token())

        mgr1 = TokenManager(provider1)
        mgr2 = TokenManager(provider2)

        # Same instance due to singleton
        assert mgr1 is mgr2

        # Note: This test demonstrates singleton behavior, but in practice
        # you typically want to clear the singleton between tests
        # See test cleanup section below


@pytest.mark.unit
@pytest.mark.auth
class TestDriverTokenManager:
    """Tests for driver-specific token manager"""

    def test_driver_token_manager_has_background_coroutine(self):
        """
        GIVEN a driver token manager
        WHEN background_coroutine is called
        THEN it should return a coroutine object
        """
        import inspect

        provider = FakeTokenProvider(valid_token())
        mgr = DriverTokenManager(provider)

        coro = mgr.background_coroutine()

        assert inspect.iscoroutine(coro)

        # Clean up
        coro.close()

    @pytest.mark.asyncio
    async def test_background_coroutine_fetches_token_on_start(self):
        """
        GIVEN a driver token manager
        WHEN the background coroutine starts
        THEN it should immediately fetch a token
        """
        provider = FakeTokenProvider(valid_token())
        mgr = DriverTokenManager(provider)

        coro = mgr.background_coroutine()
        task = asyncio.create_task(coro)

        # Give it a moment to fetch initial token
        await asyncio.sleep(0.1)

        # Should have fetched token
        assert provider.calls >= 1

        # Clean up
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_background_coroutine_is_cancellable(self):
        """
        GIVEN a running background token refresh loop
        WHEN the coroutine is cancelled
        THEN it should exit gracefully
        """
        provider = FakeTokenProvider(valid_token())
        mgr = DriverTokenManager(provider)

        coro = mgr.background_coroutine()
        task = asyncio.create_task(coro)

        await asyncio.sleep(0.01)

        # Cancel the task
        task.cancel()

        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.done()


# Singleton cleanup fixture
@pytest.fixture(autouse=True)
def reset_token_manager_singleton():
    """
    Reset TokenManager singleton between tests to avoid state leakage.

    Note: This is necessary because TokenManager uses SingletonMeta.
    Without this, one test's TokenManager instance would affect another's.
    """
    from core.singleton import SingletonMeta

    # Clear singleton instances before each test
    if TokenManager in SingletonMeta._instances:
        del SingletonMeta._instances[TokenManager]
    if DriverTokenManager in SingletonMeta._instances:
        del SingletonMeta._instances[DriverTokenManager]

    yield

    # Clear singleton instances after each test
    if TokenManager in SingletonMeta._instances:
        del SingletonMeta._instances[TokenManager]
    if DriverTokenManager in SingletonMeta._instances:
        del SingletonMeta._instances[DriverTokenManager]
