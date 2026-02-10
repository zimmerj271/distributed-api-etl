"""Integration tests for RPC services with aiohttp

This module tests both the base RpcService functionality and the
concrete TokenRpcService implementation.
"""
import pytest
import json
import asyncio
import aiohttp.web
from aiohttp.test_utils import make_mocked_request
from datetime import datetime, timedelta

from auth.rpc.service import RpcService, TokenRpcService
from auth.token.token_provider import Token
from tests.fixtures.spark import FakeSparkSession


class FakeTokenManager:
    """Token manager test double for integration testing"""
    def __init__(self, token=None, should_fail=False):
        self.token = token or Token(
            token_value="integration-test-token",
            expires_at=datetime.now() + timedelta(seconds=300)
        )
        self.should_fail = should_fail
        self.get_token_calls = 0
    
    async def get_token(self):
        self.get_token_calls += 1
        if self.should_fail:
            raise RuntimeError("Simulated token failure")
        return self.token


class MinimalRpcService(RpcService):
    """Minimal concrete RpcService for testing base class behavior"""
    
    def build_app(self) -> aiohttp.web.Application:
        """Returns a minimal aiohttp app"""
        app = aiohttp.web.Application()
        
        async def health_handler(request):
            return aiohttp.web.json_response({"status": "ok"})
        
        app.router.add_get("/health", health_handler)
        return app


# =============================================================================
# Base RpcService Integration Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.auth
class TestRpcServiceIntegration:
    """Integration tests for base RpcService functionality"""
    
    def test_rpc_service_builds_valid_aiohttp_app(self):
        """
        GIVEN a concrete RpcService implementation
        WHEN build_app is called
        THEN it should return a valid aiohttp Application
        """
        spark = FakeSparkSession()
        svc = MinimalRpcService(spark=spark, port=9999)
        
        app = svc.build_app()
        
        assert isinstance(app, aiohttp.web.Application)
        assert len(app.router.routes()) >= 1
    
    @pytest.mark.asyncio
    async def test_rpc_service_endpoint_is_callable(self):
        """
        GIVEN an RpcService with a registered endpoint
        WHEN the endpoint handler is called
        THEN it should return a valid response
        """
        spark = FakeSparkSession()
        svc = MinimalRpcService(spark=spark, port=9999)
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/health")
        response = await handler(request)
        
        assert response.status == 200
    
    @pytest.mark.asyncio
    async def test_rpc_service_background_coroutine_lifecycle(self):
        """
        GIVEN an RpcService
        WHEN background_coroutine is started and cancelled
        THEN it should handle the lifecycle correctly
        """
        spark = FakeSparkSession()
        svc = MinimalRpcService(spark=spark, port=9999)
        
        coro = svc.background_coroutine()
        task = asyncio.create_task(coro)
        
        # Let it start
        await asyncio.sleep(0.01)
        assert not task.done()
        
        # Cancel
        task.cancel()
        
        with pytest.raises(asyncio.CancelledError):
            await task
        
        assert task.done()


# =============================================================================
# TokenRpcService Integration Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.auth
class TestTokenRpcServiceAppCreation:
    """Integration tests for TokenRpcService app creation"""
    
    def test_token_rpc_service_builds_app(self):
        """
        GIVEN a TokenRpcService
        WHEN build_app is called
        THEN it should return an aiohttp Application with /token endpoint
        """
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        
        assert isinstance(app, aiohttp.web.Application)
        
        # Verify /token endpoint exists
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        assert len(routes) >= 1


@pytest.mark.integration
@pytest.mark.auth
class TestTokenEndpointBehavior:
    """Integration tests for /token endpoint behavior"""
    
    @pytest.mark.asyncio
    async def test_token_endpoint_returns_200_on_success(self):
        """
        GIVEN a TokenRpcService with working token manager
        WHEN GET /token is called
        THEN it should return 200 with token JSON
        """
        token = Token(
            token_value="test-token-123",
            expires_at=datetime(2025, 6, 1, 12, 0, 0)
        )
        token_mgr = FakeTokenManager(token=token)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        response = await handler(request)
        
        assert response.status == 200
        assert token_mgr.get_token_calls == 1
    
    @pytest.mark.asyncio
    async def test_token_endpoint_returns_valid_json_structure(self):
        """
        GIVEN a TokenRpcService
        WHEN GET /token succeeds
        THEN response should contain token_value and expires_at
        """
        token = Token(
            token_value="json-test-token",
            expires_at=datetime(2025, 6, 1, 12, 0, 0)
        )
        token_mgr = FakeTokenManager(token=token)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        response = await handler(request)
        
        body = response.body
        data = json.loads(body.decode('utf-8'))
        
        assert "token_value" in data
        assert "expires_at" in data
        assert data["token_value"] == "json-test-token"
        assert "2025-06-01" in data["expires_at"]
    
    @pytest.mark.asyncio
    async def test_token_endpoint_returns_500_on_failure(self):
        """
        GIVEN a TokenRpcService with failing token manager
        WHEN GET /token is called
        THEN it should return 500 error
        """
        token_mgr = FakeTokenManager(should_fail=True)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        response = await handler(request)
        
        assert response.status == 500
    
    @pytest.mark.asyncio
    async def test_token_endpoint_returns_error_message_on_failure(self):
        """
        GIVEN a failing token manager
        WHEN GET /token is called
        THEN response should contain error message
        """
        token_mgr = FakeTokenManager(should_fail=True)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        response = await handler(request)
        
        body = response.body
        data = json.loads(body.decode('utf-8'))
        
        assert "error" in data
        assert "retrieve token" in data["error"].lower()
    
    @pytest.mark.asyncio
    async def test_multiple_requests_call_token_manager_each_time(self):
        """
        GIVEN a TokenRpcService
        WHEN multiple GET /token requests are made
        THEN token manager should be called each time (no endpoint-level caching)
        """
        token_mgr = FakeTokenManager()
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        # Make 3 requests
        for _ in range(3):
            request = make_mocked_request("GET", "/token")
            response = await handler(request)
            assert response.status == 200
        
        # Token manager should be called 3 times
        assert token_mgr.get_token_calls == 3
    
    @pytest.mark.asyncio
    async def test_token_endpoint_handles_none_expiry(self):
        """
        GIVEN a token with no expiration
        WHEN GET /token is called
        THEN it should serialize expires_at as empty string
        """
        token = Token(token_value="no-expiry-token", expires_at=None)
        token_mgr = FakeTokenManager(token=token)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        response = await handler(request)
        
        assert response.status == 200
        
        body = response.body
        data = json.loads(body.decode('utf-8'))
        assert data["expires_at"] == ""


@pytest.mark.integration
@pytest.mark.auth
class TestTokenRpcServiceLogging:
    """Integration tests for RPC service logging behavior"""
    
    @pytest.mark.asyncio
    async def test_endpoint_logs_token_fetch_failure(self, caplog):
        """
        GIVEN a failing token manager
        WHEN GET /token is called
        THEN it should log the error
        """
        import logging
        
        caplog.set_level(logging.ERROR)
        
        token_mgr = FakeTokenManager(should_fail=True)
        
        svc = TokenRpcService(
            spark=FakeSparkSession(),
            token_manager=token_mgr,
            port=9999,
        )
        
        app = svc.build_app()
        routes = [r for r in app.router.routes() if hasattr(r, 'method') and r.method == 'GET']
        handler = routes[0].handler
        
        request = make_mocked_request("GET", "/token")
        await handler(request)
        
        # Check that error was logged
        assert any("Failed to get token" in record.message for record in caplog.records)
