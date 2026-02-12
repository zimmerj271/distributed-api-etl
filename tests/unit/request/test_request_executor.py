"""Unit tests for middleware orchestrator (RequestExecutor)"""

import pytest
from request_execution.models import (
    RequestExchange,
    RequestContext,
    RequestType,
    TransportResponse,
)
from request_execution.executor import RequestExecutor
from tests.fixtures.request_execution.transport import FakeTransportEngine
from tests.fixtures.request_execution.executor import basic_request_context


@pytest.mark.unit
@pytest.mark.middleware
class TestRequestExecutorInitialization:
    """Tests for RequestExecutor initialization"""

    def test_creates_with_transport_and_factories(self):
        """
        GIVEN a transport engine and middleware factories
        WHEN RequestExecutor is created
        THEN it should initialize successfully
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )

        client = RequestExecutor(transport, middleware_factories=[])

        assert client.transport is transport

    def test_stores_middleware_factories(self):
        """
        GIVEN middleware factories
        WHEN RequestExecutor is created
        THEN factories should be stored
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )

        async def dummy_mw(req, next_call):
            return await next_call(req)

        factories = [lambda: dummy_mw]
        client = RequestExecutor(transport, middleware_factories=factories)

        assert client._middleware_factories is factories


@pytest.mark.unit
@pytest.mark.middleware
@pytest.mark.asyncio
class TestRequestExecutorSend:
    """Tests for RequestExecutor.send() method"""

    async def test_send_executes_transport(self):
        """
        GIVEN a RequestExecutor with transport
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
        client = RequestExecutor(transport, middleware_factories=[])

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
        GIVEN a RequestExecutor with middleware factories
        WHEN send is called
        THEN middleware should execute
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )

        executed = []

        async def tracking_mw(req, next_call):
            executed.append(True)
            return await next_call(req)

        client = RequestExecutor(
            transport, middleware_factories=[lambda: tracking_mw]
        )

        ctx = RequestContext(
            method=RequestType.GET,
            url="/test",
            headers={},
        )

        await client.send(ctx)

        assert len(executed) == 1

    async def test_send_runs_multiple_middleware(self):
        """
        GIVEN a RequestExecutor with multiple middleware factories
        WHEN send is called
        THEN all middleware should execute
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )

        executed = []

        async def mw1(req, next_call):
            executed.append("mw1")
            return await next_call(req)

        async def mw2(req, next_call):
            executed.append("mw2")
            return await next_call(req)

        client = RequestExecutor(
            transport, middleware_factories=[lambda: mw1, lambda: mw2]
        )

        ctx = RequestContext(
            method=RequestType.GET,
            url="/test",
            headers={},
        )

        await client.send(ctx)

        assert len(executed) == 2

    async def test_send_builds_transport_request_correctly(self):
        """
        GIVEN a RequestExecutor
        WHEN send is called with RequestContext
        THEN TransportRequest should be built correctly
        """
        transport = FakeTransportEngine(
            TransportResponse(status=200, headers=None, body=None)
        )
        client = RequestExecutor(transport, middleware_factories=[])

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


@pytest.mark.asyncio
async def test_api_client_success_response():
    transport = FakeTransportEngine(
        TransportResponse(
            status=200,
            headers={"Content-Type": "application/json"},
            body=b'{"ok": true}',
        )
    )

    client = RequestExecutor(transport, middleware_factories=[])
    ctx = basic_request_context()

    result = await client.send(ctx)

    assert isinstance(result, RequestExchange)
    assert result.success is True
    assert result.status_code == 200
    assert result.body == b'{"ok": true}'
    assert result.headers["Content-Type"] == "application/json"


@pytest.mark.asyncio
async def test_api_client_builds_transport_request_correctly():
    transport = FakeTransportEngine(
        TransportResponse(status=200, headers=None, body=None)
    )

    client = RequestExecutor(transport, middleware_factories=[])
    ctx = basic_request_context()

    await client.send(ctx)

    req = transport.last_request
    assert req is not None

    assert req.method == "GET"
    assert req.url == "test/resource"  # leading slash stripped
    assert req.headers["X-Test"] == "1"

    if req.params is not None:
        assert req.params["q"] == "value"


@pytest.mark.asyncio
async def test_api_client_sets_success_false_on_500():
    transport = FakeTransportEngine(
        TransportResponse(status=500, headers=None, body=None)
    )

    client = RequestExecutor(transport, middleware_factories=[])
    ctx = basic_request_context()

    result = await client.send(ctx)

    assert result.success is False
    assert result.status_code == 500


@pytest.mark.asyncio
async def test_api_client_transport_error():
    transport = FakeTransportEngine(
        TransportResponse(
            status=None,
            headers=None,
            body=None,
            error="Connection failed",
        )
    )

    client = RequestExecutor(transport, middleware_factories=[])
    ctx = basic_request_context()

    result = await client.send(ctx)

    assert result.success is False
    assert result.error_message == "Connection failed"


@pytest.mark.asyncio
async def test_api_client_runs_middleware():
    async def mw(req, next_call):
        req.context.headers["X-MW"] = "yes"
        return await next_call(req)

    transport = FakeTransportEngine(
        TransportResponse(status=200, headers=None, body=None)
    )

    client = RequestExecutor(transport, middleware_factories=[lambda: mw])

    ctx = basic_request_context()
    result = await client.send(ctx)

    assert result.context.headers["X-MW"] == "yes"
