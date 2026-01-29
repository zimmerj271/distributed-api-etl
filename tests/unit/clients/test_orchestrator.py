"""Unit tests for middleware orchestrator (RequestExecutor)"""
import pytest
from request_execution import (
    RequestExecutor,
    RequestExchange,
    RequestContext,
    RequestType,
    TransportResponse
)
from tests.fixtures.request_execution import FakeTransportEngine


@pytest.mark.unit
@pytest.mark.middleware
class TestRequestExecutorInitialization:
    """Tests for RequestExecutor initialization"""
    
    def test_creates_with_transport(self):
        """
        GIVEN a transport engine
        WHEN RequestExecutor is created
        THEN it should initialize successfully
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        
        client = RequestExecutor(transport)
        
        assert client.transport is transport
        assert client._pipeline is not None
    
    def test_middleware_pipeline_is_empty_initially(self):
        """
        GIVEN a new RequestExecutor
        WHEN no middleware has been added
        THEN pipeline should be empty
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        
        client = RequestExecutor(transport)
        
        assert len(client._pipeline._middleware_list) == 0


@pytest.mark.unit
@pytest.mark.middleware
class TestRequestExecutorMiddlewareManagement:
    """Tests for adding middleware to RequestExecutor"""
    
    def test_add_middleware_registers_in_pipeline(self):
        """
        GIVEN an RequestExecutor
        WHEN add_middleware is called
        THEN middleware should be added to pipeline
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        client = RequestExecutor(transport)
        
        async def dummy_mw(req, next_call):
            return await next_call(req)
        
        client.add_middleware(dummy_mw)
        
        assert len(client._pipeline._middleware_list) == 1
    
    def test_add_multiple_middleware(self):
        """
        GIVEN an RequestExecutor
        WHEN multiple middleware are added
        THEN all should be registered
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        client = RequestExecutor(transport)
        
        async def mw1(req, next_call):
            return await next_call(req)
        
        async def mw2(req, next_call):
            return await next_call(req)
        
        client.add_middleware(mw1)
        client.add_middleware(mw2)
        
        assert len(client._pipeline._middleware_list) == 2


@pytest.mark.unit
@pytest.mark.middleware
@pytest.mark.asyncio
class TestRequestExecutorSend:
    """Tests for RequestExecutor.send() method"""
    
    async def test_send_executes_transport(self):
        """
        GIVEN an RequestExecutor with transport
        WHEN send is called
        THEN transport should execute the request
        """
        transport = FakeTransportEngine(
            TransportResponse(
                status=200,
                headers={"Content-Type": "application/json"},
                body=b'{"ok": true}',
            )
        )
        client = RequestExecutor(transport)
        
        ctx = RequestContext(
            method=RequestType.GET,
            url="/test",
            headers={},
        )
        
        result = await client.send(ctx)
        
        assert isinstance(result, RequestExchange)
        assert result.success is True
        assert result.status_code == 200
    
    async def test_send_runs_middleware_pipeline(self):
        """
        GIVEN an RequestExecutor with middleware
        WHEN send is called
        THEN middleware should execute
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        client = RequestExecutor(transport)
        
        executed = []
        
        async def tracking_mw(req, next_call):
            executed.append(True)
            return await next_call(req)
        
        client.add_middleware(tracking_mw)
        
        ctx = RequestContext(
            method=RequestType.GET,
            url="/test",
            headers={},
        )
        
        await client.send(ctx)
        
        assert len(executed) == 1
    
    async def test_send_builds_transport_request_correctly(self):
        """
        GIVEN an RequestExecutor
        WHEN send is called with RequestContext
        THEN TransportRequest should be built correctly
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        client = RequestExecutor(transport)
        
        ctx = RequestContext(
            method=RequestType.POST,
            url="/api/endpoint",
            headers={"X-Custom": "value"},
            params={"q": "search"},
            json={"data": "payload"},
        )
        
        await client.send(ctx)
        
        req = transport.last_request
        assert req is not None
        assert req.method == "POST"
        assert req.url == "api/endpoint"  # Leading slash stripped
        assert req.headers["X-Custom"] == "value"
        assert req.params["q"] == "search"
        assert req.json["data"] == "payload"
