"""Integration tests for ApiPartitionExecutor queue-based concurrency"""

import pytest
import asyncio
import threading
import json
from aiohttp import web
from pyspark.sql import Row

from orchestration.partition_executor import ApiPartitionExecutor
from request_execution.models import RequestContext, RequestType
from request_execution.transport.engine import AiohttpEngine
from request_execution.middleware.interceptors import JsonResponseMiddleware
from config.models.transport import TcpConnectionConfig


def make_rows(n: int) -> list[Row]:
    """Create n test rows with request_id and a data field."""
    return [
        Row(request_id=f"req-{i}", patient_id=f"P{i:03d}")
        for i in range(n)
    ]


def create_test_app():
    """Create a test aiohttp application that tracks concurrent requests."""
    app = web.Application()
    app["processed"] = []
    app["max_concurrent"] = 0
    app["current_concurrent"] = 0
    app["lock"] = asyncio.Lock()

    async def handle_request(request):
        async with app["lock"]:
            app["current_concurrent"] += 1
            if app["current_concurrent"] > app["max_concurrent"]:
                app["max_concurrent"] = app["current_concurrent"]

        # Simulate work
        await asyncio.sleep(0.01)

        patient_id = request.query.get("patient_id", "unknown")
        app["processed"].append(patient_id)

        async with app["lock"]:
            app["current_concurrent"] -= 1

        return web.json_response({
            "status": "ok",
            "patient_id": patient_id,
        })

    app.router.add_get("/api/patient", handle_request)
    return app


class BackgroundServer:
    """Manages an aiohttp test server running in a background thread."""

    def __init__(self, app: web.Application):
        self.app = app
        self._runner = None
        self._site = None
        self._loop = None
        self._thread = None
        self.port = None

    def start(self):
        self._loop = asyncio.new_event_loop()
        started = threading.Event()

        def run():
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._start(started))
            self._loop.run_forever()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        started.wait(timeout=5)

    async def _start(self, started: threading.Event):
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await self._site.start()
        self.port = self._site._server.sockets[0].getsockname()[1]
        started.set()

    def stop(self):
        if self._loop and self._runner:
            asyncio.run_coroutine_threadsafe(
                self._runner.cleanup(), self._loop
            ).result(timeout=5)
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture
def test_server():
    app = create_test_app()
    server = BackgroundServer(app)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def tcp_config():
    return TcpConnectionConfig(limit=10, limit_per_host=5)


@pytest.mark.integration
class TestPartitionExecutorConcurrency:
    """Tests for queue-based concurrent row processing"""

    def test_processes_all_rows(self, test_server, tcp_config):
        """
        GIVEN an ApiPartitionExecutor with rows to process
        WHEN make_map_partitions_fn is called
        THEN all rows should be processed and returned
        """
        base_url = test_server.base_url
        rows = make_rows(10)

        def transport_factory():
            return AiohttpEngine(
                base_url=base_url,
                connector_config=tcp_config,
            )

        def endpoint_factory():
            return RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/patient",
                param_mapping={"patient_id": "patient_id"},
            )

        executor = ApiPartitionExecutor(
            transport_factory=transport_factory,
            endpoint_factory=endpoint_factory,
            middleware_factories=[lambda: JsonResponseMiddleware()],
            concurrency_limit=5,
            param_mapping={"patient_id": "patient_id"},
        )

        fn = executor.make_map_partitions_fn()
        result_rows = fn(iter(rows))

        assert len(result_rows) == 10

        result_request_ids = {r["request_id"] for r in result_rows}
        expected_request_ids = {f"req-{i}" for i in range(10)}
        assert result_request_ids == expected_request_ids

    def test_concurrent_consumers_are_bounded(self, test_server, tcp_config):
        """
        GIVEN an ApiPartitionExecutor with concurrency_limit=3
        WHEN processing multiple rows
        THEN the maximum concurrent requests should not exceed the limit
        """
        base_url = test_server.base_url
        rows = make_rows(15)

        def transport_factory():
            return AiohttpEngine(
                base_url=base_url,
                connector_config=tcp_config,
            )

        def endpoint_factory():
            return RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/patient",
                param_mapping={"patient_id": "patient_id"},
            )

        concurrency_limit = 3

        executor = ApiPartitionExecutor(
            transport_factory=transport_factory,
            endpoint_factory=endpoint_factory,
            middleware_factories=[lambda: JsonResponseMiddleware()],
            concurrency_limit=concurrency_limit,
            param_mapping={"patient_id": "patient_id"},
        )

        fn = executor.make_map_partitions_fn()
        result_rows = fn(iter(rows))

        assert len(result_rows) == 15
        assert test_server.app["max_concurrent"] <= concurrency_limit

    def test_result_rows_contain_response_data(self, test_server, tcp_config):
        """
        GIVEN an ApiPartitionExecutor
        WHEN rows are processed through the pipeline
        THEN result rows should contain response metadata (status_code, json_body, etc.)
        """
        base_url = test_server.base_url
        rows = make_rows(3)

        def transport_factory():
            return AiohttpEngine(
                base_url=base_url,
                connector_config=tcp_config,
            )

        def endpoint_factory():
            return RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/patient",
                param_mapping={"patient_id": "patient_id"},
            )

        executor = ApiPartitionExecutor(
            transport_factory=transport_factory,
            endpoint_factory=endpoint_factory,
            middleware_factories=[lambda: JsonResponseMiddleware()],
            concurrency_limit=5,
            param_mapping={"patient_id": "patient_id"},
        )

        fn = executor.make_map_partitions_fn()
        result_rows = fn(iter(rows))

        for row in result_rows:
            assert row["status_code"] == 200
            assert row["success"] is True
            assert row["json_body"] is not None

    def test_handles_empty_partition(self, test_server, tcp_config):
        """
        GIVEN an ApiPartitionExecutor
        WHEN an empty partition is processed
        THEN it should return an empty list
        """
        base_url = test_server.base_url

        def transport_factory():
            return AiohttpEngine(
                base_url=base_url,
                connector_config=tcp_config,
            )

        def endpoint_factory():
            return RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/patient",
            )

        executor = ApiPartitionExecutor(
            transport_factory=transport_factory,
            endpoint_factory=endpoint_factory,
            middleware_factories=[],
            concurrency_limit=5,
        )

        fn = executor.make_map_partitions_fn()
        result_rows = fn(iter([]))

        assert result_rows == []

    def test_single_row_partition(self, test_server, tcp_config):
        """
        GIVEN an ApiPartitionExecutor
        WHEN a partition with a single row is processed
        THEN it should return exactly one result row
        """
        base_url = test_server.base_url
        rows = make_rows(1)

        def transport_factory():
            return AiohttpEngine(
                base_url=base_url,
                connector_config=tcp_config,
            )

        def endpoint_factory():
            return RequestContext(
                method=RequestType.GET,
                url=f"{base_url}/api/patient",
                param_mapping={"patient_id": "patient_id"},
            )

        executor = ApiPartitionExecutor(
            transport_factory=transport_factory,
            endpoint_factory=endpoint_factory,
            middleware_factories=[lambda: JsonResponseMiddleware()],
            concurrency_limit=5,
            param_mapping={"patient_id": "patient_id"},
        )

        fn = executor.make_map_partitions_fn()
        result_rows = fn(iter(rows))

        assert len(result_rows) == 1
        assert result_rows[0]["request_id"] == "req-0"
        assert result_rows[0]["status_code"] == 200
