"""Unit tests for TokenRpcService"""
import pytest
import aiohttp.web
from datetime import datetime, timedelta

from auth.rpc.service import TokenRpcService
from auth.token.token_provider import Token
from tests.fixtures.spark import FakeSparkSession


class FakeTokenManager:
    """Minimal fake token manager for unit testing"""
    def __init__(self, token=None, should_fail=False):
        self.token = token or Token(
            token_value="unit-test-token",
            expires_at=datetime.now() + timedelta(seconds=300)
        )
        self.should_fail = should_fail
        self.get_token_calls = 0
    
    async def get_token(self):
        self.get_token_calls += 1
        if self.should_fail:
            raise RuntimeError("Token fetch failed")
        return self.token


@pytest.mark.unit
@pytest.mark.auth
class TestTokenRpcServiceInitialization:
    """Tests for TokenRpcService initialization"""
    
    def test_creates_service_with_valid_port(self):
        """
        GIVEN valid parameters
        WHEN TokenRpcService is created
        THEN it should initialize successfully
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        
        svc = TokenRpcService(
            spark=spark,
            token_manager=token_mgr,
            port=9999
        )
        
        assert svc.port == 9999
        assert svc.url == "http://localhost:9999"
    
    def test_creates_service_with_dynamic_port(self):
        """
        GIVEN no explicit port
        WHEN TokenRpcService is created
        THEN it should assign a dynamic port
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        
        svc = TokenRpcService(
            spark=spark,
            token_manager=token_mgr,
            port=None  # Dynamic
        )
        
        assert 1024 < svc.port <= 65535
    
    def test_rejects_invalid_port(self):
        """
        GIVEN an invalid/reserved port
        WHEN TokenRpcService is created
        THEN it should raise ValueError
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        
        with pytest.raises(ValueError, match="not a valid port"):
            TokenRpcService(
                spark=spark,
                token_manager=token_mgr,
                port=80  # Reserved port
            )


@pytest.mark.unit
@pytest.mark.auth
class TestTokenRpcServiceAppCreation:
    """Tests for aiohttp app creation"""
    
    def test_build_app_returns_application(self):
        """
        GIVEN a TokenRpcService
        WHEN build_app is called
        THEN it should return an aiohttp Application
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(spark=spark, token_manager=token_mgr, port=9999)
        
        app = svc.build_app()
        
        assert isinstance(app, aiohttp.web.Application)
    
    def test_build_app_registers_token_endpoint(self):
        """
        GIVEN a TokenRpcService
        WHEN build_app is called
        THEN it should register the /token endpoint
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(spark=spark, token_manager=token_mgr, port=9999)
        
        app = svc.build_app()
        
        # Check routes
        routes = list(app.router.routes())
        get_routes = [r for r in routes if hasattr(r, 'method') and r.method == 'GET']
        
        assert len(get_routes) >= 1
        # Check that /token path exists
        route_paths = [r.resource.canonical for r in get_routes if hasattr(r.resource, 'canonical')]
        assert any('/token' in path for path in route_paths)


@pytest.mark.unit
@pytest.mark.auth
class TestTokenRpcServiceBackgroundCoroutine:
    """Tests for background coroutine"""
    
    def test_background_coroutine_returns_coroutine(self):
        """
        GIVEN a TokenRpcService
        WHEN background_coroutine is called
        THEN it should return a coroutine object
        """
        import inspect
        
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(spark=spark, token_manager=token_mgr, port=9999)
        
        coro = svc.background_coroutine()
        
        assert inspect.iscoroutine(coro)
        
        # Clean up
        coro.close()
    
    @pytest.mark.asyncio
    async def test_background_coroutine_is_cancellable(self):
        """
        GIVEN a running background coroutine
        WHEN it is cancelled
        THEN it should exit gracefully with CancelledError
        """
        import asyncio
        
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(spark=spark, token_manager=token_mgr, port=9999)
        
        coro = svc.background_coroutine()
        task = asyncio.create_task(coro)
        
        # Let it start
        await asyncio.sleep(0.01)
        
        # Cancel
        task.cancel()
        
        with pytest.raises(asyncio.CancelledError):
            await task
        
        assert task.done()


@pytest.mark.unit
@pytest.mark.auth
class TestTokenRpcServiceWaitUntilReady:
    """Tests for wait_until_ready mechanism"""
    
    def test_wait_until_ready_before_start_raises(self):
        """
        GIVEN a TokenRpcService that hasn't started
        WHEN wait_until_ready is called with timeout
        THEN it should raise TimeoutError
        """
        spark = FakeSparkSession()
        token_mgr = FakeTokenManager()
        svc = TokenRpcService(spark=spark, token_manager=token_mgr, port=9999)
        
        with pytest.raises(TimeoutError, match="failed to start"):
            svc.wait_until_ready(timeout=0.1)
