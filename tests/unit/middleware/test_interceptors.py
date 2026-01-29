"""Unit tests for interceptor middleware"""
import pytest
from pyspark.sql import Row
from unittest.mock import patch, AsyncMock
import json

from request_execution import (
    RetryMiddleware,
    JsonResponseMiddleware,
    ParamInjectorMiddleware,
)
from tests.fixtures.request_execution import base_exchange, base_request_context


@pytest.mark.unit
@pytest.mark.middleware
class TestRetryMiddleware:
    """Tests for retry middleware behavior"""
    
    @pytest.mark.asyncio
    async def test_succeeds_on_first_attempt(self):
        """
        GIVEN retry middleware with max_attempts=3
        WHEN request succeeds on first try
        THEN it should not retry
        """
        mw = RetryMiddleware(max_attempts=3)

        async def success_handler(req):
            req.status_code = 200
            req.success = True
            return req

        req = base_exchange()
        result = await mw(req, success_handler)

        assert result.success is True
        assert result.attempts == 1
        assert result.status_code == 200
    
    @pytest.mark.asyncio
    async def test_retries_on_retryable_status_code(self):
        """
        GIVEN retry middleware configured to retry on 500
        WHEN handler returns 500 status
        THEN it should retry up to max_attempts
        """
        mw = RetryMiddleware(max_attempts=3, retry_status_codes=[500])
        call_count = 0

        async def always_500(req):
            nonlocal call_count
            call_count += 1
            req.status_code = 500
            req.success = False
            return req

        req = base_exchange()
        result = await mw(req, always_500)

        assert call_count == 3
        assert result.attempts == 3
        assert result.success is False
        assert result.metadata["retry_attempts"] == 3
    
    @pytest.mark.asyncio
    async def test_succeeds_after_retries(self):
        """
        GIVEN retry middleware
        WHEN first attempt fails but second succeeds
        THEN it should return success
        """
        mw = RetryMiddleware(max_attempts=3, retry_status_codes=[500])
        attempt = 0

        async def flaky_handler(req):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                req.status_code = 500
                req.success = False
            else:
                req.status_code = 200
                req.success = True
            return req

        req = base_exchange()
        result = await mw(req, flaky_handler)

        assert result.success is True
        assert result.attempts == 2
        assert result.status_code == 200
    
    @pytest.mark.asyncio
    async def test_does_not_retry_on_non_retryable_status(self):
        """
        GIVEN retry middleware with specific retry codes
        WHEN handler returns non-retryable status
        THEN it should not retry
        """
        mw = RetryMiddleware(max_attempts=3, retry_status_codes=[500, 502])
        call_count = 0

        async def returns_404(req):
            nonlocal call_count
            call_count += 1
            req.status_code = 404
            req.success = False
            return req

        req = base_exchange()
        result = await mw(req, returns_404)

        assert call_count == 1  # No retries
        assert result.attempts == 1
        assert result.status_code == 404
    
    @pytest.mark.asyncio
    async def test_retries_on_retryable_exception(self):
        """
        GIVEN retry middleware
        WHEN handler raises a retryable exception
        THEN it should retry
        """
        import aiohttp
        
        mw = RetryMiddleware(max_attempts=3)
        attempt = 0

        async def flaky_exception_handler(req):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise aiohttp.ClientConnectionError("Connection failed")
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, flaky_exception_handler)

        assert result.status_code == 200
        assert result.attempts == 2
    
    @pytest.mark.asyncio
    async def test_exhausts_retries_on_persistent_exception(self):
        """
        GIVEN retry middleware with max_attempts=3
        WHEN handler always raises retryable exception
        THEN it should exhaust retries and return error
        """
        import aiohttp
        
        mw = RetryMiddleware(max_attempts=3)

        async def always_fails(req):
            raise aiohttp.ClientConnectionError("Always fails")

        req = base_exchange()
        result = await mw(req, always_fails)

        assert result.success is False
        assert result.attempts == 3
        assert "Retry attempts exhausted" in result.error_message
    
    @pytest.mark.asyncio
    async def test_does_not_retry_non_retryable_exception(self):
        """
        GIVEN retry middleware
        WHEN handler raises non-retryable exception
        THEN it should not retry
        """
        mw = RetryMiddleware(max_attempts=3)
        call_count = 0

        async def raises_value_error(req):
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        req = base_exchange()
        result = await mw(req, raises_value_error)

        assert call_count == 1
        assert result.success is False
        assert "Non-retryable exception" in result.error_message
    
    @pytest.mark.asyncio
    @patch("middleware.interceptors.asyncio.sleep", new_callable=AsyncMock)
    async def test_applies_exponential_backoff(self, mock_sleep):
        """
        GIVEN retry middleware with backoff settings
        WHEN retries occur
        THEN delays should increase exponentially
        """
        mw = RetryMiddleware(max_attempts=3, base_delay=0.1, max_delay=2.0)

        async def always_500(req):
            req.status_code = 500
            return req

        req = base_exchange()
        await mw(req, always_500)

        # Should have slept during retries
        assert mock_sleep.call_count >= 2


@pytest.mark.unit
@pytest.mark.middleware
class TestJsonResponseMiddleware:
    """Tests for JSON response parsing middleware"""
    
    @pytest.mark.asyncio
    async def test_parses_valid_json(self):
        """
        GIVEN JSON response middleware
        WHEN response contains valid JSON
        THEN it should parse and mark as valid
        """
        mw = JsonResponseMiddleware()

        async def handler(req):
            req.body = b'{"foo": "bar", "num": 42}'
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, handler)

        assert result.metadata["json"]["valid"] is True
        assert result.metadata["json"]["error"] is None
        assert result.body_text == '{"foo": "bar", "num": 42}'
    
    @pytest.mark.asyncio
    async def test_handles_invalid_json(self):
        """
        GIVEN JSON response middleware
        WHEN response contains invalid JSON
        THEN it should mark as invalid with error
        """
        mw = JsonResponseMiddleware()

        async def handler(req):
            req.body = b'{bad json'
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, handler)

        assert result.metadata["json"]["valid"] is False
        assert result.metadata["json"]["error"] is not None
    
    @pytest.mark.asyncio
    async def test_handles_empty_body(self):
        """
        GIVEN JSON response middleware
        WHEN response has no body
        THEN it should handle gracefully
        """
        mw = JsonResponseMiddleware()

        async def handler(req):
            req.body = None
            req.status_code = 204
            return req

        req = base_exchange()
        result = await mw(req, handler)

        # Should not crash
        assert "json" not in result.metadata or result.metadata.get("json") is None
    
    @pytest.mark.asyncio
    async def test_sets_success_true_on_2xx_with_valid_json(self):
        """
        GIVEN JSON response middleware
        WHEN response is 2xx with valid JSON
        THEN success should be True
        """
        mw = JsonResponseMiddleware()

        async def handler(req):
            req.body = b'{"status": "ok"}'
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, handler)

        assert result.success is True
    
    @pytest.mark.asyncio
    async def test_converts_body_to_text(self):
        """
        GIVEN JSON response middleware
        WHEN response has binary body
        THEN it should decode to text
        """
        mw = JsonResponseMiddleware()

        async def handler(req):
            req.body = b'{"data": "value"}'
            req.status_code = 200
            return req

        req = base_exchange()
        result = await mw(req, handler)

        assert isinstance(result.body_text, str)
        assert result.body_text == '{"data": "value"}'


@pytest.mark.unit
@pytest.mark.middleware
class TestParamInjectorMiddleware:
    """Tests for parameter injection middleware"""
    
    @pytest.mark.asyncio
    async def test_injects_params_from_row(self):
        """
        GIVEN param injector middleware with bindings
        WHEN request has row data
        THEN params should be injected from row
        """
        mw = ParamInjectorMiddleware({"patient_id": "tulip_id"})

        req = base_exchange()
        req.context._row = Row(tulip_id="P123", other_field="value")

        async def handler(r):
            return r

        result = await mw(req, handler)

        assert result.context.params is not None
        assert result.context.params["patient_id"] == "P123"
    
    @pytest.mark.asyncio
    async def test_injects_multiple_params(self):
        """
        GIVEN param injector with multiple bindings
        WHEN request has row data
        THEN all params should be injected
        """
        mw = ParamInjectorMiddleware({
            "patient": "patient_id",
            "encounter": "encounter_id"
        })

        req = base_exchange()
        req.context._row = Row(patient_id="P123", encounter_id="E456")

        async def handler(r):
            return r

        result = await mw(req, handler)

        assert result.context.params["patient"] == "P123"
        assert result.context.params["encounter"] == "E456"
    
    @pytest.mark.asyncio
    async def test_handles_missing_row(self):
        """
        GIVEN param injector middleware
        WHEN request has no row data
        THEN it should not crash
        """
        mw = ParamInjectorMiddleware({"patient": "patient_id"})

        req = base_exchange()
        req.context._row = None

        async def handler(r):
            return r

        result = await mw(req, handler)

        # Should not crash, just pass through
        assert result is not None
    
    @pytest.mark.asyncio
    async def test_clears_existing_params(self):
        """
        GIVEN param injector middleware
        WHEN request already has params
        THEN it should clear and replace them
        """
        mw = ParamInjectorMiddleware({"patient": "patient_id"})

        req = base_exchange()
        req.context.params = {"old_param": "old_value"}
        req.context._row = Row(patient_id="P123")

        async def handler(r):
            return r

        result = await mw(req, handler)

        assert "old_param" not in result.context.params
        assert result.context.params["patient"] == "P123"
