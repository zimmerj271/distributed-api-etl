"""Unit tests for middleware pipeline execution"""

import pytest
from request_execution import MiddlewarePipeline, MIDDLEWARE_FUNC
from tests.fixtures.request_execution.middleware import (
    base_exchange,
    terminal_handler_ok,
    terminal_handler_fail,
)


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineExecution:
    """Tests for middleware pipeline execution order and flow"""

    @pytest.mark.asyncio
    async def test_executes_middleware_in_order(self):
        """
        GIVEN a pipeline with multiple middleware
        WHEN execute is called
        THEN middleware should be called in registration order
        """
        calls = []

        async def mw1(req, next_call):
            calls.append("mw1")
            return await next_call(req)

        async def mw2(req, next_call):
            calls.append("mw2")
            return await next_call(req)

        async def mw3(req, next_call):
            calls.append("mw3")
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(mw1)
        pipeline.add(mw2)
        pipeline.add(mw3)

        await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert calls == ["mw1", "mw2", "mw3"]

    @pytest.mark.asyncio
    async def test_reaches_terminal_handler(self):
        """
        GIVEN a pipeline with middleware
        WHEN execute is called
        THEN it should reach the terminal handler
        """
        pipeline = MiddlewarePipeline()
        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_empty_pipeline_calls_terminal_directly(self):
        """
        GIVEN an empty pipeline (no middleware)
        WHEN execute is called
        THEN it should call terminal handler directly
        """
        pipeline = MiddlewarePipeline()

        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert result.status_code == 200


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineDataFlow:
    """Tests for data flow through middleware pipeline"""

    @pytest.mark.asyncio
    async def test_middleware_can_modify_request(self):
        """
        GIVEN middleware that modifies the request
        WHEN execute is called
        THEN modifications should propagate through pipeline
        """

        async def add_header(req, next_call):
            req.context.headers["X-Custom"] = "value"
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(add_header)

        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert result.context.headers["X-Custom"] == "value"

    @pytest.mark.asyncio
    async def test_middleware_can_modify_response(self):
        """
        GIVEN middleware that modifies the response
        WHEN execute is called
        THEN modifications should be in final result
        """

        async def add_metadata(req, next_call):
            result = await next_call(req)
            result.metadata["processed"] = True
            return result

        pipeline = MiddlewarePipeline()
        pipeline.add(add_metadata)

        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert result.metadata["processed"] is True

    @pytest.mark.asyncio
    async def test_multiple_middleware_can_modify_same_data(self):
        """
        GIVEN multiple middleware that modify the same data
        WHEN execute is called
        THEN all modifications should be applied in order
        """

        async def mw1(req, next_call):
            req.context.headers["Counter"] = "1"
            return await next_call(req)

        async def mw2(req, next_call):
            current = req.context.headers["Counter"]
            req.context.headers["Counter"] = current + ",2"
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(mw1)
        pipeline.add(mw2)

        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert result.context.headers["Counter"] == "1,2"


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineShortCircuit:
    """Tests for middleware short-circuit behavior"""

    @pytest.mark.asyncio
    async def test_middleware_can_short_circuit(self):
        """
        GIVEN middleware that doesn't call next
        WHEN execute is called
        THEN downstream middleware should not execute
        """
        calls = []

        async def short_circuit(req, next_call):
            calls.append("short_circuit")
            req.status_code = 403
            req.success = False
            return req  # Don't call next_call

        async def never_called(req, next_call):
            calls.append("never_called")
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(short_circuit)
        pipeline.add(never_called)

        result = await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert calls == ["short_circuit"]
        assert result.status_code == 403
        assert result.success is False

    @pytest.mark.asyncio
    async def test_short_circuit_prevents_terminal_handler(self):
        """
        GIVEN middleware that short-circuits
        WHEN execute is called
        THEN terminal handler should not be called
        """
        terminal_called = []

        async def short_circuit(req, next_call):
            req.status_code = 401
            return req

        async def tracking_terminal(req):
            terminal_called.append(True)
            return req

        pipeline = MiddlewarePipeline()
        pipeline.add(short_circuit)

        await pipeline.execute(base_exchange(), tracking_terminal)

        assert len(terminal_called) == 0


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineNesting:
    """Tests for middleware nesting (wrap model)"""

    @pytest.mark.asyncio
    async def test_middleware_wraps_downstream(self):
        """
        GIVEN middleware that runs code before and after next_call
        WHEN execute is called
        THEN both before and after code should execute
        """
        order = []

        async def wrapper(req, next_call):
            order.append("before")
            result = await next_call(req)
            order.append("after")
            return result

        pipeline = MiddlewarePipeline()
        pipeline.add(wrapper)

        await pipeline.execute(base_exchange(), terminal_handler_ok)

        assert order == ["before", "after"]

    @pytest.mark.asyncio
    async def test_nested_middleware_execution_order(self):
        """
        GIVEN multiple wrapping middleware
        WHEN execute is called
        THEN execution should follow nested order: outer before, inner before, terminal, inner after, outer after
        """
        order = []

        async def outer(req, next_call):
            order.append("outer_before")
            result = await next_call(req)
            order.append("outer_after")
            return result

        async def inner(req, next_call):
            order.append("inner_before")
            result = await next_call(req)
            order.append("inner_after")
            return result

        async def terminal(req):
            order.append("terminal")
            return req

        pipeline = MiddlewarePipeline()
        pipeline.add(outer)
        pipeline.add(inner)

        await pipeline.execute(base_exchange(), terminal)

        assert order == [
            "outer_before",
            "inner_before",
            "terminal",
            "inner_after",
            "outer_after",
        ]


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineErrorHandling:
    """Tests for error handling in middleware pipeline"""

    @pytest.mark.asyncio
    async def test_middleware_exception_propagates(self):
        """
        GIVEN middleware that raises an exception
        WHEN execute is called
        THEN the exception should propagate
        """

        async def failing_middleware(req, next_call):
            raise ValueError("Middleware failed")

        pipeline = MiddlewarePipeline()
        pipeline.add(failing_middleware)

        with pytest.raises(ValueError, match="Middleware failed"):
            await pipeline.execute(base_exchange(), terminal_handler_ok)

    @pytest.mark.asyncio
    async def test_terminal_handler_exception_propagates(self):
        """
        GIVEN a terminal handler that raises an exception
        WHEN execute is called
        THEN the exception should propagate through middleware
        """

        async def failing_terminal(req):
            raise RuntimeError("Terminal failed")

        pipeline = MiddlewarePipeline()

        with pytest.raises(RuntimeError, match="Terminal failed"):
            await pipeline.execute(base_exchange(), failing_terminal)

    @pytest.mark.asyncio
    async def test_middleware_can_catch_downstream_exceptions(self):
        """
        GIVEN middleware that catches exceptions from downstream
        WHEN downstream raises
        THEN middleware can handle it gracefully
        """

        async def error_handler(req, next_call):
            try:
                return await next_call(req)
            except ValueError:
                req.success = False
                req.error_message = "Handled error"
                return req

        async def failing_terminal(req):
            raise ValueError("Terminal error")

        pipeline = MiddlewarePipeline()
        pipeline.add(error_handler)

        result = await pipeline.execute(base_exchange(), failing_terminal)

        assert result.success is False
        assert result.error_message == "Handled error"


@pytest.mark.unit
@pytest.mark.middleware
class TestMiddlewarePipelineAddition:
    """Tests for adding middleware to pipeline"""

    def test_add_middleware_to_pipeline(self):
        """
        GIVEN a middleware pipeline
        WHEN add is called
        THEN middleware should be registered
        """

        async def dummy_mw(req, next_call):
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(dummy_mw)

        assert len(pipeline._middleware_list) == 1
        assert pipeline._middleware_list[0] is dummy_mw

    def test_add_multiple_middleware(self):
        """
        GIVEN a middleware pipeline
        WHEN add is called multiple times
        THEN all middleware should be registered in order
        """

        async def mw1(req, next_call):
            return await next_call(req)

        async def mw2(req, next_call):
            return await next_call(req)

        pipeline = MiddlewarePipeline()
        pipeline.add(mw1)
        pipeline.add(mw2)

        assert len(pipeline._middleware_list) == 2
        assert pipeline._middleware_list[0] is mw1
        assert pipeline._middleware_list[1] is mw2
