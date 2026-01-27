import pytest

from clients.http_client import ApiClient
from clients.base import RequestExchange
from transport.base import TransportResponse
from tests.fixtures.transport import FakeTransportEngine
from tests.fixtures.clients import basic_request_context


@pytest.mark.asyncio
async def test_api_client_success_response():
    transport = FakeTransportEngine(
        TransportResponse(
            status=200,
            headers={"Content-Type": "application/json"},
            body=b'{"ok": true}',
        )
    )

    client = ApiClient(transport)
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

    client = ApiClient(transport)
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

    client = ApiClient(transport)
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

    client = ApiClient(transport)
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

    client = ApiClient(transport)
    client.add_middleware(mw)

    ctx = basic_request_context()
    result = await client.send(ctx)

    assert result.context.headers["X-MW"] == "yes"

