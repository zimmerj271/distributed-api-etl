"""Integration tests for RequestExecutor with middleware and transport"""
import pytest
import json
from aiohttp import web

from request_execution import (
    RequestExecutor,
    RequestContext,
    RequestType,
    RetryMiddleware,
    JsonResponseMiddleware,
    LoggingMiddleware,
    TimingMiddleware,
)
from request_execution.transport.engine import AiohttpEngine
from config.models.transport import TcpConnectionConfig


@pytest.fixture
def tcp_config():
    """Basic TCP configuration for tests"""
    return TcpConnectionConfig(limit=10, limit_per_host=5)


def create_test_app():
    """Create a test aiohttp application"""
    app = web.Application()

    async def handle_success(request):
        return web.json_response({
            "status": "ok",
            "message": "Request processed successfully",
            "request_id": request.query.get("id", "unknown"),
        })

    async def handle_error(request):
        return web.json_response(
            {"error": "Bad request"},
            status=400,
        )

    async def handle_server_error(request):
        return web.json_response(
            {"error": "Internal server error"},
            status=500,
        )

    async def handle_flaky(request):
        """Endpoint that fails first 2 times, then succeeds"""
        app["flaky_count"] = app.get("flaky_count", 0) + 1
        if app["flaky_count"] <= 2:
            return web.json_response({"error": "temporary"}, status=503)
        return web.json_response({"status": "ok", "attempt": app["flaky_count"]})

    async def handle_echo(request):
        """Echo back request details"""
        body = None
        if request.body_exists:
            body = await request.json()

        return web.json_response({
            "method": request.method,
            "path": str(request.path),
            "query": dict(request.query),
            "headers": dict(request.headers),
            "body": body,
        })

    app.router.add_get("/api/success", handle_success)
    app.router.add_get("/api/error", handle_error)
    app.router.add_get("/api/server-error", handle_server_error)
    app.router.add_get("/api/flaky", handle_flaky)
    app.router.add_route("*", "/api/echo", handle_echo)

    return app


@pytest.mark.integration
@pytest.mark.asyncio
class TestRequestExecutorBasicFlow:
    """Tests for basic request execution flow"""

    async def test_executes_simple_get_request(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with transport
        WHEN a simple GET request is executed
        THEN the response should be captured in RequestExchange
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/success",
                params={"id": "test-123"},
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 200
            assert exchange.success is True
            assert exchange.body is not None

    async def test_captures_error_response(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with transport
        WHEN a request returns an error status
        THEN the error should be captured correctly
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/error",
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 400
            # 400 is < 500, so transport considers it "success" (no transport error)
            assert exchange.error_message is None

    async def test_post_request_with_json_body(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with transport
        WHEN a POST request with JSON body is executed
        THEN the body should be sent and response received
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.POST,
                url=f"{base_url}/api/echo",
                headers={"Content-Type": "application/json"},
                json={"data": "test-value", "count": 42},
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 200
            body = json.loads(exchange.body)
            assert body["method"] == "POST"
            assert body["body"]["data"] == "test-value"
            assert body["body"]["count"] == 42


@pytest.mark.integration
@pytest.mark.asyncio
class TestRequestExecutorWithMiddleware:
    """Tests for RequestExecutor with middleware pipeline"""

    async def test_json_middleware_parses_response(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with JsonResponseMiddleware
        WHEN a request returns JSON
        THEN the response should be parsed and validated
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(JsonResponseMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/success",
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 200
            assert exchange.body_text is not None
            assert exchange.metadata["json"]["valid"] is True

    async def test_timing_middleware_captures_duration(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with TimingMiddleware
        WHEN a request is executed
        THEN the duration should be captured in metadata
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(TimingMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/success",
            )

            exchange = await executor.send(context)

            assert "timing" in exchange.metadata
            assert "total_seconds" in exchange.metadata["timing"]
            assert exchange.metadata["timing"]["total_seconds"] >= 0

    async def test_logging_middleware_captures_logs(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with LoggingMiddleware
        WHEN a request is executed
        THEN request/response should be logged in metadata
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(LoggingMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/success",
            )

            exchange = await executor.send(context)

            assert "logs" in exchange.metadata
            logs = exchange.metadata["logs"]
            # Should have request and response logs
            assert len(logs) >= 2
            assert any("GET" in log for log in logs)

    async def test_retry_middleware_retries_on_failure(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with RetryMiddleware
        WHEN a flaky endpoint is called
        THEN the middleware should retry until success
        """
        app = create_test_app()
        app["flaky_count"] = 0  # Reset counter
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=5,
                    retry_status_codes=[503],
                    base_delay=0.01,  # Fast retries for testing
                )
            )

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/flaky",
            )

            exchange = await executor.send(context)

            # Should succeed after retries
            assert exchange.status_code == 200
            assert exchange.success is True
            assert exchange.attempts == 3  # Failed twice, succeeded on third

    async def test_multiple_middleware_execute_in_order(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestExecutor with multiple middleware
        WHEN a request is executed
        THEN all middleware should execute and contribute to metadata
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            # Add middleware in order: logging -> timing -> json
            executor.add_middleware(LoggingMiddleware())
            executor.add_middleware(TimingMiddleware())
            executor.add_middleware(JsonResponseMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/success",
            )

            exchange = await executor.send(context)

            # All middleware should have contributed
            assert "logs" in exchange.metadata
            assert "timing" in exchange.metadata
            assert "json" in exchange.metadata


@pytest.mark.integration
@pytest.mark.asyncio
class TestRequestExecutorContextHandling:
    """Tests for request context handling"""

    async def test_headers_are_forwarded(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestContext with custom headers
        WHEN the request is executed
        THEN the headers should be sent to the server
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/echo",
                headers={
                    "X-Custom-Header": "test-value",
                    "X-Request-Id": "req-123",
                },
            )

            exchange = await executor.send(context)

            body = json.loads(exchange.body)
            assert body["headers"]["X-Custom-Header"] == "test-value"
            assert body["headers"]["X-Request-Id"] == "req-123"

    async def test_params_are_forwarded(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestContext with query parameters
        WHEN the request is executed
        THEN the params should be sent to the server
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/echo",
                params={"foo": "bar", "count": "10"},
            )

            exchange = await executor.send(context)

            body = json.loads(exchange.body)
            assert body["query"]["foo"] == "bar"
            assert body["query"]["count"] == "10"

    async def test_url_path_handling(self, aiohttp_client, tcp_config):
        """
        GIVEN a RequestContext with a full URL
        WHEN the request is executed
        THEN the path should be correctly used
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            context = RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/echo",
            )

            exchange = await executor.send(context)

            body = json.loads(exchange.body)
            assert "/api/echo" in body["path"]


@pytest.mark.integration
@pytest.mark.asyncio
class TestRequestExecutorRowConversion:
    """Tests for converting RequestExchange to PySpark Row"""

    async def test_build_row_creates_valid_row(self, aiohttp_client, tcp_config):
        """
        GIVEN a completed RequestExchange
        WHEN build_row is called
        THEN a valid PySpark Row should be created
        """
        app = create_test_app()
        client = await aiohttp_client(app)
        base_url = str(client.make_url(""))

        engine = AiohttpEngine(
            base_url=base_url,
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(JsonResponseMiddleware())
            executor.add_middleware(TimingMiddleware())

            full_url = f"{base_url}/api/success"
            context = RequestContext(
                method=RequestType.GET,
                url=full_url,
                params={"id": "test-123"},
            )

            exchange = await executor.send(context)

            # Convert to Row
            row = exchange.build_row(request_id_value="test-123")

            # Verify row fields
            assert row["request_id"] == "test-123"
            assert row["url"] == full_url
            assert row["method"] == "GET"
            assert row["status_code"] == 200
            assert row["success"] is True
            assert row["json_body"] is not None
            assert row["row_hash"] is not None  # Hash of response body
