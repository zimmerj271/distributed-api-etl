import pytest
from pyspark.sql import Row
from middleware.interceptors import RetryMiddleware, JsonResponseMiddleware, ParamInjectorMiddleware
from tests.fixtures.middleware import base_exchange


@pytest.mark.asyncio
async def test_retry_middleware_retries_on_status():
    mw = RetryMiddleware(max_attempts=2)

    async def flaky(req):
        req.status_code = 500
        return req

    req = base_exchange()
    result = await mw(req, flaky)

    assert result.success is False
    assert result.metadata["retry_attempts"] == 2


@pytest.mark.asyncio
async def test_retry_middleware_succeeds():
    mw = RetryMiddleware(max_attempts=3)

    async def ok(req):
        req.status_code = 200
        return req

    req = base_exchange()
    result = await mw(req, ok)

    assert result.success is True
    assert result.attempts == 1


@pytest.mark.asyncio
async def test_json_response_middleware_valid_json():
    mw = JsonResponseMiddleware()

    async def handler(req):
        req.body = b'{"foo": "bar"}'
        req.status_code = 200
        return req

    req = base_exchange()
    result = await mw(req, handler)

    assert result.metadata["json"]["valid"] is True


@pytest.mark.asyncio
async def test_json_response_middleware_invalid_json():
    mw = JsonResponseMiddleware()

    async def handler(req):
        req.body = b"{bad json"
        req.status_code = 200
        return req

    req = base_exchange()
    result = await mw(req, handler)

    assert result.metadata["json"]["valid"] is False


@pytest.mark.asyncio
async def test_param_injector_middleware_sets_params():
    mw = ParamInjectorMiddleware({"q": "col"})

    req = base_exchange()
    req.context._row = Row(col="value")

    async def handler(r):
        return r

    result = await mw(req, handler)

    if result.context.params is not None:
        assert result.context.params["q"] == "value"
