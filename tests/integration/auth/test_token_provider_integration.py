"""Integration tests for token providers"""
import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from aiohttp import web

from auth.token.models import Token
from auth.token.token_provider import RpcTokenProvider, FallbackTokenProvider, StaticTokenProvider
from auth.token.token_manager import TokenManager
from auth.rpc.service import TokenRpcService
from tests.fixtures.spark import FakeSparkSession


@pytest.fixture
def valid_token():
    """Create a valid token that expires in 1 hour"""
    return Token(
        token_value="test-token-value",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )


@pytest.fixture
def expiring_token():
    """Create a token that expires in 30 seconds"""
    return Token(
        token_value="expiring-token",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )


class FakeTokenManager:
    """Test double for TokenManager"""

    def __init__(self, token: Token = None, should_fail: bool = False):
        self._token = token
        self._should_fail = should_fail
        self.call_count = 0

    async def get_token(self) -> Token:
        self.call_count += 1
        if self._should_fail:
            raise RuntimeError("Token acquisition failed")
        return self._token


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestRpcTokenProviderWithService:
    """Tests for RpcTokenProvider fetching from actual TokenRpcService"""

    async def test_fetches_token_from_rpc_service(self, aiohttp_client, valid_token):
        """
        GIVEN a TokenRpcService running with a valid token
        WHEN RpcTokenProvider fetches a token
        THEN the token should be returned correctly
        """
        token_manager = FakeTokenManager(token=valid_token)
        rpc_service = TokenRpcService(spark=FakeSparkSession(), token_manager=token_manager)
        app = rpc_service.build_app()

        client = await aiohttp_client(app)

        # Create RpcTokenProvider pointing to test server
        # Note: RpcTokenProvider appends /token to the rpc_url, so we pass the base URL
        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        token = await provider.get_token()

        assert token.token_value == "test-token-value"
        assert token.expires_at is not None
        assert token_manager.call_count == 1

    async def test_multiple_fetches_call_service_each_time(self, aiohttp_client, valid_token):
        """
        GIVEN a TokenRpcService running
        WHEN multiple token fetches are made
        THEN each fetch should call the service (no caching at provider level)
        """
        token_manager = FakeTokenManager(token=valid_token)
        rpc_service = TokenRpcService(spark=FakeSparkSession(), token_manager=token_manager)
        app = rpc_service.build_app()

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        # Fetch multiple times
        for _ in range(3):
            await provider.get_token()

        # Each fetch should hit the service
        assert token_manager.call_count == 3

    async def test_handles_service_error(self, aiohttp_client):
        """
        GIVEN a TokenRpcService that returns an error
        WHEN RpcTokenProvider fetches a token
        THEN an exception should be raised after retries
        """
        token_manager = FakeTokenManager(should_fail=True)
        rpc_service = TokenRpcService(spark=FakeSparkSession(), token_manager=token_manager)
        app = rpc_service.build_app()

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
            max_retries=2,
            base_delay=0.01,
        )

        with pytest.raises(RuntimeError):
            await provider.get_token()

    async def test_retries_on_network_error(self, aiohttp_client, valid_token):
        """
        GIVEN a TokenRpcService that fails initially then succeeds
        WHEN RpcTokenProvider fetches a token
        THEN it should retry and eventually succeed
        """
        call_count = 0

        async def flaky_token_handler(request):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return web.json_response({"error": "temporary"}, status=503)
            return web.json_response({
                "token_value": valid_token.token_value,
                "expires_at": valid_token.expires_at.isoformat(),
            })

        app = web.Application()
        app.router.add_get("/token", flaky_token_handler)

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
            max_retries=5,
            base_delay=0.01,
        )

        token = await provider.get_token()

        assert token.token_value == valid_token.token_value
        assert call_count == 3  # 2 failures + 1 success


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestFallbackTokenProvider:
    """Tests for FallbackTokenProvider behavior"""

    async def test_uses_primary_when_available(self, aiohttp_client, valid_token):
        """
        GIVEN a FallbackTokenProvider with working primary
        WHEN a token is fetched
        THEN the primary provider should be used
        """
        token_manager = FakeTokenManager(token=valid_token)
        rpc_service = TokenRpcService(spark=FakeSparkSession(), token_manager=token_manager)
        app = rpc_service.build_app()

        client = await aiohttp_client(app)

        primary = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        fallback = StaticTokenProvider("fallback-token")

        provider = FallbackTokenProvider(primary=primary, fallback=fallback)

        token = await provider.get_token()

        assert token.token_value == "test-token-value"
        # Verify primary was used via telemetry
        telemetry = provider.token_telemetry()
        assert telemetry["provider"] == "RpcTokenProvider"

    async def test_uses_fallback_when_primary_fails(self):
        """
        GIVEN a FallbackTokenProvider with failing primary
        WHEN a token is fetched
        THEN the fallback provider should be used
        """
        # Primary that will fail (bad URL)
        primary = RpcTokenProvider(
            rpc_url="http://localhost:59999/token",  # Non-existent
            timeout=0.5,
            max_retries=1,
            base_delay=0.01,
        )

        fallback = StaticTokenProvider("fallback-token")

        provider = FallbackTokenProvider(primary=primary, fallback=fallback)

        token = await provider.get_token()

        assert token.token_value == "fallback-token"
        # Verify fallback was used via telemetry
        telemetry = provider.token_telemetry()
        assert telemetry["provider"] == "StaticTokenProvider"


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestStaticTokenProvider:
    """Tests for StaticTokenProvider"""

    async def test_returns_static_token(self):
        """
        GIVEN a StaticTokenProvider with a token string
        WHEN get_token is called
        THEN a token with that value should be returned
        """
        provider = StaticTokenProvider("static-token-123")

        result = await provider.get_token()

        assert result.token_value == "static-token-123"
        assert result.expires_at == datetime.max.replace(tzinfo=timezone.utc)

    async def test_returns_same_token_on_multiple_calls(self):
        """
        GIVEN a StaticTokenProvider
        WHEN get_token is called multiple times
        THEN tokens with the same value should be returned
        """
        provider = StaticTokenProvider("static-token")

        results = [await provider.get_token() for _ in range(5)]

        # All should have the same token value
        assert all(r.token_value == "static-token" for r in results)
        assert all(r.expires_at == datetime.max.replace(tzinfo=timezone.utc) for r in results)


@pytest.mark.integration
@pytest.mark.auth
@pytest.mark.asyncio
class TestTokenProviderWithRealTokenManager:
    """Tests for token providers integrated with real TokenManager"""

    async def test_token_manager_caches_token(self, aiohttp_client, valid_token):
        """
        GIVEN a TokenManager wrapping an RpcTokenProvider
        WHEN multiple token requests are made
        THEN the TokenManager should cache and reuse the token
        """
        call_count = 0

        async def counting_token_handler(request):
            nonlocal call_count
            call_count += 1
            return web.json_response({
                "token_value": valid_token.token_value,
                "expires_at": valid_token.expires_at.isoformat(),
            })

        app = web.Application()
        app.router.add_get("/token", counting_token_handler)

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        # Clear singleton state for testing
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        # Multiple requests should hit cache
        for _ in range(5):
            await manager.get_token()

        # Only one actual RPC call should have been made
        assert call_count == 1

    async def test_token_manager_refreshes_near_expiry(self, aiohttp_client):
        """
        GIVEN a TokenManager with a token near expiry
        WHEN get_token is called
        THEN the token should be refreshed
        """
        call_count = 0

        async def token_handler(request):
            nonlocal call_count
            call_count += 1
            # Return token that expires in 30 seconds
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=30)
            return web.json_response({
                "token_value": f"token-{call_count}",
                "expires_at": expires_at.isoformat(),
            })

        app = web.Application()
        app.router.add_get("/token", token_handler)

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        # Clear singleton state for testing
        TokenManager._instances.clear()
        # Set refresh margin to 60 seconds, so token expiring in 30s will trigger refresh
        manager = TokenManager(provider=provider, refresh_margin=60)

        # First call
        token1 = await manager.get_token()
        assert token1.token_value == "token-1"

        # Second call should refresh because token will expire within margin
        token2 = await manager.get_token()
        assert token2.token_value == "token-2"

        assert call_count == 2

    async def test_concurrent_refresh_serialized(self, aiohttp_client, valid_token):
        """
        GIVEN a TokenManager with no cached token
        WHEN multiple concurrent requests are made
        THEN only one actual token fetch should occur
        """
        call_count = 0

        async def slow_token_handler(request):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)  # Simulate slow token fetch
            return web.json_response({
                "token_value": valid_token.token_value,
                "expires_at": valid_token.expires_at.isoformat(),
            })

        app = web.Application()
        app.router.add_get("/token", slow_token_handler)

        client = await aiohttp_client(app)

        provider = RpcTokenProvider(
            rpc_url=str(client.make_url("")),
            timeout=5.0,
        )

        # Clear singleton state for testing
        TokenManager._instances.clear()
        manager = TokenManager(provider=provider, refresh_margin=60)

        # Launch concurrent requests
        tasks = [manager.get_token() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # All should get the same token
        assert all(r.token_value == valid_token.token_value for r in results)

        # Only one actual fetch should have occurred (serialized by lock)
        assert call_count == 1
