"""Integration tests for middleware chain interactions"""
import pytest
import json
import asyncio
from aiohttp import web

from request_execution import (
    RequestExecutor,
    RequestContext,
    RequestExchange,
    RequestType,
    MiddlewarePipeline,
    MIDDLEWARE_FUNC,
    RetryMiddleware,
    JsonResponseMiddleware,
    LoggingMiddleware,
    TimingMiddleware,
    WorkerIdentityMiddleware,
    ParamInjectorMiddleware,
    HeaderAuthMiddleware,
)
from request_execution.transport.engine import AiohttpEngine
from config.models.transport import TcpConnectionConfig
from pyspark.sql import Row


@pytest.fixture
def tcp_config():
    """Basic TCP configuration for tests"""
    return TcpConnectionConfig(limit=10, limit_per_host=5)


def create_test_app():
    """Create a test aiohttp application for middleware testing"""
    app = web.Application()
    app["request_count"] = 0
    app["auth_failures"] = 0

    async def handle_success(request):
        app["request_count"] += 1
        return web.json_response({
            "status": "ok",
            "request_number": app["request_count"],
        })

    async def handle_protected(request):
        """Endpoint requiring authorization"""
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            app["auth_failures"] += 1
            return web.json_response({"error": "Unauthorized"}, status=401)
        return web.json_response({"status": "authorized"})

    async def handle_flaky_then_success(request):
        """Fails first N times based on query param, then succeeds"""
        fail_count = int(request.query.get("fail_count", "2"))
        app["request_count"] += 1

        if app["request_count"] <= fail_count:
            return web.json_response(
                {"error": "temporary", "attempt": app["request_count"]},
                status=503,
            )
        return web.json_response({
            "status": "ok",
            "final_attempt": app["request_count"],
        })

    async def handle_slow(request):
        """Slow endpoint for timing tests"""
        delay = float(request.query.get("delay", "0.1"))
        await asyncio.sleep(delay)
        return web.json_response({"delayed": delay})

    async def handle_echo(request):
        """Echo all request details"""
        return web.json_response({
            "method": request.method,
            "headers": dict(request.headers),
            "query": dict(request.query),
        })

    app.router.add_get("/api/success", handle_success)
    app.router.add_get("/api/protected", handle_protected)
    app.router.add_get("/api/flaky", handle_flaky_then_success)
    app.router.add_get("/api/slow", handle_slow)
    app.router.add_get("/api/echo", handle_echo)

    return app


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestMiddlewareChainOrder:
    """Tests for middleware execution order"""

    async def test_middleware_execute_in_order(self, aiohttp_client, tcp_config):
        """
        GIVEN a middleware pipeline with multiple middleware
        WHEN a request is executed
        THEN middleware should execute in the order they were added
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        # Track middleware execution order
        execution_order = []

        class OrderTrackingMiddleware:
            def __init__(self, name):
                self.name = name

            async def __call__(self, exchange, next_call):
                execution_order.append(f"{self.name}_before")
                result = await next_call(exchange)
                execution_order.append(f"{self.name}_after")
                return result

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(OrderTrackingMiddleware("first"))
            executor.add_middleware(OrderTrackingMiddleware("second"))
            executor.add_middleware(OrderTrackingMiddleware("third"))

            context = RequestContext(
                method=RequestType.GET,
                url="/api/success",
            )

            await executor.send(context)

            # Verify execution order (onion model)
            assert execution_order == [
                "first_before",
                "second_before",
                "third_before",
                "third_after",
                "second_after",
                "first_after",
            ]


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestRetryWithOtherMiddleware:
    """Tests for retry middleware interacting with other middleware"""

    async def test_retry_middleware_retries_entire_chain(self, aiohttp_client, tcp_config):
        """
        GIVEN retry middleware wrapping other middleware
        WHEN retries occur
        THEN the entire downstream chain should be re-executed
        """
        app = create_test_app()
        app["request_count"] = 0
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        timing_calls = []

        class TimingTracker:
            async def __call__(self, exchange, next_call):
                timing_calls.append("timing_called")
                return await next_call(exchange)

        async with engine:
            executor = RequestExecutor(transport=engine)
            # Retry wraps timing - timing should be called on each retry
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=5,
                    retry_status_codes=[503],
                    base_delay=0.01,
                )
            )
            executor.add_middleware(TimingTracker())

            context = RequestContext(
                method=RequestType.GET,
                url="/api/flaky",
                params={"fail_count": "2"},  # Fail twice
            )

            exchange = await executor.send(context)

            # Should have 3 timing calls (2 failures + 1 success)
            assert len(timing_calls) == 3
            assert exchange.status_code == 200
            assert exchange.attempts == 3

    async def test_retry_with_logging_captures_all_attempts(self, aiohttp_client, tcp_config):
        """
        GIVEN retry middleware with logging middleware
        WHEN retries occur
        THEN logs should capture all retry attempts
        """
        app = create_test_app()
        app["request_count"] = 0
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(LoggingMiddleware())
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=5,
                    retry_status_codes=[503],
                    base_delay=0.01,
                )
            )

            context = RequestContext(
                method=RequestType.GET,
                url="/api/flaky",
                params={"fail_count": "2"},
            )

            exchange = await executor.send(context)

            # Logs should show the request/response flow
            logs = exchange.metadata.get("logs", [])
            assert len(logs) >= 2  # At least request and response


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestAuthMiddlewareIntegration:
    """Tests for authentication middleware integration"""

    async def test_header_auth_middleware_injects_credentials(self, aiohttp_client, tcp_config):
        """
        GIVEN HeaderAuthMiddleware in the pipeline
        WHEN a request is made to a protected endpoint
        THEN credentials should be injected and request should succeed
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(HeaderAuthMiddleware("user", "pass"))

            context = RequestContext(
                method=RequestType.GET,
                url="/api/protected",
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 200
            body = json.loads(exchange.body)
            assert body["status"] == "authorized"

    async def test_auth_with_retry_retries_with_auth(self, aiohttp_client, tcp_config):
        """
        GIVEN auth middleware before retry middleware
        WHEN retries occur
        THEN auth should be applied on each retry
        """
        app = create_test_app()
        app["request_count"] = 0
        client = await aiohttp_client(app)

        # Custom endpoint that requires auth AND is flaky
        async def handle_flaky_protected(request):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Basic "):
                return web.json_response({"error": "Unauthorized"}, status=401)

            app["request_count"] += 1
            if app["request_count"] <= 2:
                return web.json_response({"error": "temporary"}, status=503)
            return web.json_response({"status": "ok"})

        app._router.add_route("GET", "/api/flaky-protected", handle_flaky_protected)
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            # Auth must be before retry so it's applied on each retry
            executor.add_middleware(HeaderAuthMiddleware("user", "pass"))
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=5,
                    retry_status_codes=[503],
                    base_delay=0.01,
                )
            )

            context = RequestContext(
                method=RequestType.GET,
                url="/api/flaky-protected",
            )

            exchange = await executor.send(context)

            assert exchange.status_code == 200
            assert exchange.attempts == 3


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestParamInjectorMiddleware:
    """Tests for parameter injection from Spark rows"""

    async def test_injects_params_from_row(self, aiohttp_client, tcp_config):
        """
        GIVEN ParamInjectorMiddleware with bindings
        WHEN a request context has a Spark row
        THEN params should be injected from the row
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(
                ParamInjectorMiddleware({"patient_id": "pid", "encounter": "enc"})
            )

            context = RequestContext(
                method=RequestType.GET,
                url="/api/echo",
            )
            # Simulate a Spark row
            context._row = Row(pid="P123", enc="E456", other="ignored")

            exchange = await executor.send(context)

            body = json.loads(exchange.body)
            assert body["query"]["patient_id"] == "P123"
            assert body["query"]["encounter"] == "E456"
            assert "other" not in body["query"]


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestTimingMiddlewareAccuracy:
    """Tests for timing middleware accuracy"""

    async def test_timing_captures_actual_duration(self, aiohttp_client, tcp_config):
        """
        GIVEN TimingMiddleware in the pipeline
        WHEN a slow request is made
        THEN the timing should reflect the actual duration
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(TimingMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url="/api/slow",
                params={"delay": "0.2"},  # 200ms delay
            )

            exchange = await executor.send(context)

            timing = exchange.metadata.get("timing", {})
            total_seconds = timing.get("total_seconds", 0)

            # Should be at least 0.2 seconds
            assert total_seconds >= 0.2
            # But not too much more (allow for overhead)
            assert total_seconds < 0.5


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestWorkerIdentityMiddleware:
    """Tests for worker identity middleware"""

    async def test_adds_worker_identity_metadata(self, aiohttp_client, tcp_config):
        """
        GIVEN WorkerIdentityMiddleware in the pipeline
        WHEN a request is made
        THEN worker identity should be added to metadata
        """
        app = create_test_app()
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)
            executor.add_middleware(WorkerIdentityMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url="/api/success",
            )

            exchange = await executor.send(context)

            identity = exchange.metadata.get("executor_identity", {})
            assert "hostname" in identity
            assert "pid" in identity
            assert "thread_id" in identity
            assert identity["hostname"] is not None
            assert identity["pid"] > 0


@pytest.mark.integration
@pytest.mark.middleware
@pytest.mark.asyncio
class TestFullMiddlewareStack:
    """Tests for complete middleware stack as used in production"""

    async def test_production_middleware_stack(self, aiohttp_client, tcp_config):
        """
        GIVEN a full production-like middleware stack
        WHEN a request is made
        THEN all middleware should contribute correctly
        """
        app = create_test_app()
        app["request_count"] = 0
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            # Add middleware in production order
            executor.add_middleware(LoggingMiddleware())
            executor.add_middleware(TimingMiddleware())
            executor.add_middleware(WorkerIdentityMiddleware())
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=3,
                    retry_status_codes=[503],
                    base_delay=0.01,
                )
            )
            executor.add_middleware(JsonResponseMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url="/api/success",
            )

            exchange = await executor.send(context)

            # Verify all middleware contributed
            assert exchange.status_code == 200
            assert exchange.success is True

            # Logging
            assert "logs" in exchange.metadata

            # Timing
            assert "timing" in exchange.metadata
            assert "total_seconds" in exchange.metadata["timing"]

            # Worker identity
            assert "executor_identity" in exchange.metadata
            assert "hostname" in exchange.metadata["executor_identity"]

            # JSON parsing
            assert "json" in exchange.metadata
            assert exchange.metadata["json"]["valid"] is True
            assert exchange.body_text is not None

    async def test_middleware_stack_with_retries(self, aiohttp_client, tcp_config):
        """
        GIVEN a full middleware stack with a flaky endpoint
        WHEN retries occur
        THEN all middleware should still function correctly
        """
        app = create_test_app()
        app["request_count"] = 0
        client = await aiohttp_client(app)

        engine = AiohttpEngine(
            base_url=str(client.make_url("")),
            connector_config=tcp_config,
        )

        async with engine:
            executor = RequestExecutor(transport=engine)

            executor.add_middleware(LoggingMiddleware())
            executor.add_middleware(TimingMiddleware())
            executor.add_middleware(
                RetryMiddleware(
                    max_attempts=5,
                    retry_status_codes=[503],
                    base_delay=0.01,
                )
            )
            executor.add_middleware(JsonResponseMiddleware())

            context = RequestContext(
                method=RequestType.GET,
                url="/api/flaky",
                params={"fail_count": "2"},
            )

            exchange = await executor.send(context)

            # Should succeed after retries
            assert exchange.status_code == 200
            assert exchange.success is True
            assert exchange.attempts == 3

            # All metadata should be present
            assert "logs" in exchange.metadata
            assert "timing" in exchange.metadata
            assert "json" in exchange.metadata
