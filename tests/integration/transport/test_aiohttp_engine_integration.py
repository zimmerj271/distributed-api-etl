"""Integration tests for AiohttpEngine with actual HTTP communication"""
import pytest
import json
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from request_execution.transport.engine import AiohttpEngine
from request_execution.transport.base import TransportRequest
from config.models.transport import TcpConnectionConfig


@pytest.fixture
def tcp_config():
    """Basic TCP configuration for tests"""
    return TcpConnectionConfig(limit=10, limit_per_host=5)


def create_test_app():
    """Create a test aiohttp application with various endpoints"""
    app = web.Application()

    async def handle_get(request):
        return web.json_response({"method": "GET", "path": str(request.path)})

    async def handle_post(request):
        body = await request.json()
        return web.json_response({"method": "POST", "received": body})

    async def handle_echo_headers(request):
        return web.json_response(dict(request.headers))

    async def handle_echo_params(request):
        return web.json_response(dict(request.query))

    async def handle_status(request):
        status = int(request.match_info["status"])
        return web.json_response({"status": status}, status=status)

    async def handle_slow(request):
        import asyncio
        await asyncio.sleep(2)
        return web.json_response({"slow": True})

    async def handle_large_response(request):
        data = {"items": [{"id": i, "value": f"item_{i}"} for i in range(1000)]}
        return web.json_response(data)

    app.router.add_get("/api/get", handle_get)
    app.router.add_post("/api/post", handle_post)
    app.router.add_get("/api/headers", handle_echo_headers)
    app.router.add_get("/api/params", handle_echo_params)
    app.router.add_get("/api/status/{status}", handle_status)
    app.router.add_get("/api/slow", handle_slow)
    app.router.add_get("/api/large", handle_large_response)

    return app


@pytest.mark.integration
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineHttpCommunication:
    """Tests for actual HTTP request/response flow"""

    async def test_get_request_returns_response(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine connected to a test server
        WHEN a GET request is made
        THEN the response should contain the expected data
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url=f"{base_url}/api/get",
                headers={},
            )

            response = await engine.send(request)

            assert response.status == 200
            assert response.error is None
            body = json.loads(response.body)
            assert body["method"] == "GET"

    async def test_post_request_sends_json_body(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine connected to a test server
        WHEN a POST request is made with JSON body
        THEN the server should receive the JSON data
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="POST",
                url=f"{base_url}/api/post",
                headers={"Content-Type": "application/json"},
                json={"key": "value", "number": 42},
            )

            response = await engine.send(request)

            assert response.status == 200
            body = json.loads(response.body)
            assert body["received"]["key"] == "value"
            assert body["received"]["number"] == 42

    async def test_custom_headers_are_sent(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine with custom headers
        WHEN a request is made
        THEN the headers should be received by the server
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url=f"{base_url}/api/headers",
                headers={
                    "X-Custom-Header": "custom-value",
                    "Authorization": "Bearer test-token",
                },
            )

            response = await engine.send(request)

            assert response.status == 200
            body = json.loads(response.body)
            assert body["X-Custom-Header"] == "custom-value"
            assert body["Authorization"] == "Bearer test-token"

    async def test_query_params_are_sent(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine with query parameters
        WHEN a request is made
        THEN the params should be received by the server
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url=f"{base_url}/api/params",
                headers={},
                params={"foo": "bar", "baz": "123"},
            )

            response = await engine.send(request)

            assert response.status == 200
            body = json.loads(response.body)
            assert body["foo"] == "bar"
            assert body["baz"] == "123"

    async def test_handles_various_status_codes(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine connected to a test server
        WHEN responses with different status codes are received
        THEN the status codes should be correctly captured
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            for expected_status in [200, 201, 400, 404, 500, 503]:
                request = TransportRequest(
                    method="GET",
                    url=f"{base_url}/api/status/{expected_status}",
                    headers={},
                )

                response = await engine.send(request)

                assert response.status == expected_status
                assert response.error is None

    async def test_handles_large_response(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine connected to a test server
        WHEN a large response is received
        THEN the entire response body should be captured
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url=f"{base_url}/api/large",
                headers={},
            )

            response = await engine.send(request)

            assert response.status == 200
            body = json.loads(response.body)
            assert len(body["items"]) == 1000


@pytest.mark.integration
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineLifecycle:
    """Tests for engine lifecycle management"""

    async def test_session_created_on_enter(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine
        WHEN entering the async context
        THEN a session should be created
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        assert engine._session is None

        async with engine:
            assert engine._session is not None
            assert not engine._session.closed

        # Session should be closed after exiting
        assert engine._session is None

    async def test_multiple_requests_reuse_session(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine with an active session
        WHEN multiple requests are made
        THEN they should reuse the same session
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            session_id = id(engine._session)

            for _ in range(5):
                request = TransportRequest(
                    method="GET",
                    url=f"{base_url}/api/get",
                    headers={},
                )
                await engine.send(request)

                # Session should be the same object
                assert id(engine._session) == session_id

    async def test_connector_created_lazily(self, tcp_config):
        """
        GIVEN an AiohttpEngine
        WHEN created but not entered
        THEN connector should be None (lazy initialization)
        """
        engine = AiohttpEngine(
            base_url="http://example.com",
            connector_config=tcp_config,
        )

        assert engine._connector is None


@pytest.mark.integration
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineErrorHandling:
    """Tests for error handling scenarios"""

    async def test_connection_refused_returns_error(self, tcp_config):
        """
        GIVEN an AiohttpEngine pointing to a non-existent server
        WHEN a request is made
        THEN the response should contain an error
        """
        engine = AiohttpEngine(
            base_url="http://localhost:59999",  # Unlikely to be in use
            connector_config=tcp_config,
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url="http://localhost:59999/api/test",
                headers={},
            )

            response = await engine.send(request)

            assert response.status is None
            assert response.error is not None
            assert "ClientConnectorError" in response.error or "Cannot connect" in response.error

    async def test_timeout_returns_error(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine with a short timeout
        WHEN a slow endpoint is called
        THEN the response should contain a timeout error
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
            base_timeout=1,  # 1 second timeout
        )

        async with engine:
            request = TransportRequest(
                method="GET",
                url=f"{base_url}/api/slow",  # Takes 2 seconds
                headers={},
            )

            response = await engine.send(request)

            assert response.status is None
            assert response.error is not None
            # Should contain timeout-related error
            assert "timeout" in response.error.lower() or "TimeoutError" in response.error


@pytest.mark.integration
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineConnectionPooling:
    """Tests for connection pooling behavior"""

    async def test_respects_connection_limit(self, aiohttp_client):
        """
        GIVEN an AiohttpEngine with connection limits
        WHEN the engine is created
        THEN the connector should respect those limits
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        config = TcpConnectionConfig(limit=25, limit_per_host=5)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=config,
        )

        async with engine:
            assert engine._connector.limit == 25
            assert engine._connector.limit_per_host == 5

    async def test_concurrent_requests_use_pool(self, aiohttp_client, tcp_config):
        """
        GIVEN an AiohttpEngine with connection pooling
        WHEN multiple concurrent requests are made
        THEN they should complete successfully using the pool
        """
        import asyncio

        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            # Make 10 concurrent requests
            async def make_request(i):
                request = TransportRequest(
                    method="GET",
                    url=f"{base_url}/api/status/200",
                    headers={},
                    params={"request_id": str(i)},
                )
                return await engine.send(request)

            responses = await asyncio.gather(*[make_request(i) for i in range(10)])

            # All requests should succeed
            assert all(r.status == 200 for r in responses)
            assert all(r.error is None for r in responses)
