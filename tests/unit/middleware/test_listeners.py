"""Unit tests for listener middleware"""

import pytest
from request_execution import (
    LoggingMiddleware,
    TimingMiddleware,
    WorkerIdentityMiddleware,
)
from tests.fixtures.request_execution.middleware import (
    base_exchange,
    terminal_handler_ok,
)


@pytest.mark.unit
@pytest.mark.middleware
class TestLoggingMiddleware:
    """Tests for logging middleware"""

    @pytest.mark.asyncio
    async def test_logs_outgoing_request(self):
        """
        GIVEN logging middleware
        WHEN request is processed
        THEN it should log the outgoing request
        """
        mw = LoggingMiddleware()
        req = base_exchange()

        result = await mw(req, terminal_handler_ok)

        logs = result.metadata["logs"]
        assert any("GET" in log and "->" in log for log in logs)

    @pytest.mark.asyncio
    async def test_logs_incoming_response(self):
        """
        GIVEN logging middleware
        WHEN response is received
        THEN it should log the response
        """
        mw = LoggingMiddleware()
        req = base_exchange()

        result = await mw(req, terminal_handler_ok)

        logs = result.metadata["logs"]
        assert any("200" in log and "<-" in log for log in logs)

    @pytest.mark.asyncio
    async def test_logs_failure(self):
        """
        GIVEN logging middleware
        WHEN request fails
        THEN it should log the failure
        """
        mw = LoggingMiddleware()

        async def failing_handler(req):
            req.status_code = None
            req.success = False
            req.error_message = "Connection failed"
            return req

        req = base_exchange()
        result = await mw(req, failing_handler)

        logs = result.metadata["logs"]
        assert any("FAILED" in log for log in logs)

    @pytest.mark.asyncio
    async def test_preserves_existing_logs(self):
        """
        GIVEN logging middleware with existing logs
        WHEN middleware runs
        THEN it should append to existing logs
        """
        mw = LoggingMiddleware()
        req = base_exchange()
        req.metadata["logs"] = ["Existing log"]

        result = await mw(req, terminal_handler_ok)

        logs = result.metadata["logs"]
        assert "Existing log" in logs
        assert len(logs) > 1


@pytest.mark.unit
@pytest.mark.middleware
class TestTimingMiddleware:
    """Tests for timing middleware"""

    @pytest.mark.asyncio
    async def test_records_total_duration(self):
        """
        GIVEN timing middleware
        WHEN request is processed
        THEN it should record total duration
        """
        mw = TimingMiddleware()
        req = base_exchange()

        result = await mw(req, terminal_handler_ok)

        assert "timing" in result.metadata
        assert "total_seconds" in result.metadata["timing"]
        assert result.metadata["timing"]["total_seconds"] >= 0.0

    @pytest.mark.asyncio
    async def test_duration_is_reasonable(self):
        """
        GIVEN timing middleware
        WHEN request completes quickly
        THEN duration should be small
        """
        import asyncio

        mw = TimingMiddleware()

        async def fast_handler(req):
            await asyncio.sleep(0.01)  # 10ms
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, fast_handler)

        duration = result.metadata["timing"]["total_seconds"]
        assert 0 <= duration <= 0.1  # Should be around 10ms

    @pytest.mark.asyncio
    async def test_preserves_existing_timing_metadata(self):
        """
        GIVEN existing timing metadata
        WHEN timing middleware runs
        THEN it should preserve existing data
        """
        mw = TimingMiddleware()
        req = base_exchange()
        req.metadata["timing"] = {"custom_metric": 123}

        result = await mw(req, terminal_handler_ok)

        assert result.metadata["timing"]["custom_metric"] == 123
        assert "total_seconds" in result.metadata["timing"]


@pytest.mark.unit
@pytest.mark.middleware
class TestWorkerIdentityMiddleware:
    """Tests for worker identity middleware"""

    @pytest.mark.asyncio
    async def test_adds_worker_identity(self):
        """
        GIVEN worker identity middleware
        WHEN request is processed
        THEN it should add executor identity metadata
        """
        mw = WorkerIdentityMiddleware()
        req = base_exchange()

        result = await mw(req, terminal_handler_ok)

        assert "executor_identity" in result.metadata
        identity = result.metadata["executor_identity"]

        assert "hostname" in identity
        assert "pid" in identity
        assert "thread_id" in identity

    @pytest.mark.asyncio
    async def test_identity_includes_executor_id(self):
        """
        GIVEN worker identity middleware
        WHEN running on Spark executor
        THEN it should include executor_id
        """
        mw = WorkerIdentityMiddleware()
        req = base_exchange()

        result = await mw(req, terminal_handler_ok)

        identity = result.metadata["executor_identity"]
        # May be None if not running on Spark
        assert "executor_id" in identity

    @pytest.mark.asyncio
    async def test_identity_is_consistent_across_calls(self):
        """
        GIVEN worker identity middleware instance
        WHEN used for multiple requests
        THEN identity should be consistent
        """
        mw = WorkerIdentityMiddleware()

        req1 = base_exchange()
        result1 = await mw(req1, terminal_handler_ok)

        req2 = base_exchange()
        result2 = await mw(req2, terminal_handler_ok)

        id1 = result1.metadata["executor_identity"]
        id2 = result2.metadata["executor_identity"]

        # Same middleware instance = same identity
        assert id1["hostname"] == id2["hostname"]
        assert id1["pid"] == id2["pid"]
        assert id1["thread_id"] == id2["thread_id"]
