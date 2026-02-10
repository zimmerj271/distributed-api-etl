"""Tests for RPC service base functionality"""
import inspect
import pytest
import aiohttp.web
from auth.rpc.service import RpcService
from tests.fixtures.spark import FakeSparkSession


class MinimalRpcService(RpcService):
    """Minimal concrete RpcService for testing abstract base class"""
    
    def build_app(self) -> aiohttp.web.Application:
        """Minimal app implementation - returns empty app"""
        return aiohttp.web.Application()


@pytest.mark.unit
class TestRpcServicePortValidation:
    """Tests for port validation logic"""
    
    @pytest.mark.parametrize("port", [0, 22, 80, 443, 4040, 7077])
    def test_invalid_ports(self, port):
        """System and Spark-reserved ports should be rejected"""
        svc = MinimalRpcService(spark=FakeSparkSession(), port=None)
        assert svc.valid_port(port) is False

    @pytest.mark.parametrize("port", [1025, 9001, 25000])
    def test_valid_ports(self, port):
        """Unreserved high ports should be accepted"""
        svc = MinimalRpcService(spark=FakeSparkSession(), port=None)
        assert svc.valid_port(port) is True


@pytest.mark.unit
class TestRpcServicePortSelection:
    """Tests for dynamic port selection"""
    
    def test_dynamic_port_select_returns_valid_port(self):
        """Dynamic port selection should return an available port"""
        port = RpcService.dynamic_port_select()
        assert 0 < port <= 65535
    

@pytest.mark.unit
class TestRpcServiceInitialization:
    """Tests for RpcService initialization"""
    
    def test_rpc_service_url_format(self):
        """RPC service should construct correct URL from host and port"""
        spark = FakeSparkSession()
        svc = MinimalRpcService(spark=spark, port=9999)
        assert svc.url == "http://localhost:9999"
    
    def test_rpc_service_rejects_invalid_port(self):
        """Initialization should reject invalid ports"""
        spark = FakeSparkSession()
        with pytest.raises(ValueError, match="not a valid port"):
            MinimalRpcService(spark=spark, port=80)  # Reserved port
    
    def test_rpc_service_accepts_none_port(self):
        """Initialization should accept None and assign dynamic port"""
        spark = FakeSparkSession()
        svc = MinimalRpcService(spark=spark, port=None)
        assert 1024 < svc.port <= 65535


@pytest.mark.unit
class TestRpcServiceBackgroundCoroutine:
    """Tests for background coroutine creation"""
    
    def test_background_coroutine_returns_coroutine(self):
        """background_coroutine should return an awaitable coroutine"""
        svc = MinimalRpcService(spark=FakeSparkSession(), port=9999)
        coro = svc.background_coroutine()
        
        # Verify it's a coroutine
        # assert hasattr(coro, "__await__")
        assert inspect.iscoroutine(coro)
        
        # Close it to avoid warning
        coro.close()
    
    @pytest.mark.asyncio
    async def test_background_coroutine_can_be_started(self):
        """Background coroutine should be startable in an event loop"""
        import asyncio
        
        svc = MinimalRpcService(spark=FakeSparkSession(), port=9999)
        coro = svc.background_coroutine()
        
        # Create a task but cancel it immediately
        task = asyncio.create_task(coro)
        await asyncio.sleep(0.01)  # Let it start
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        

