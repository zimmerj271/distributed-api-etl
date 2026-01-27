import pytest
from aiohttp.test_utils import make_mocked_request

from auth.rpc.service import TokenRpcService
from tests.fixtures.spark import FakeSparkSession
from tests.fixtures.rpc import FakeTokenManager
from auth.rpc.service import RpcService


def test_token_rpc_build_app():
    svc = TokenRpcService(
        spark=FakeSparkSession(),
        token_manager=FakeTokenManager(),
        port=9999,
    )

    app = svc.build_app()
    assert app is not None
    assert len(app.router.routes()) == 1


@pytest.mark.asyncio
async def test_token_rpc_success():
    svc = TokenRpcService(
        spark=FakeSparkSession(),
        token_manager=FakeTokenManager(),
        port=9999,
    )

    app = svc.build_app()
    route = next(iter(app.router.routes()))
    handler = route.handler

    request = make_mocked_request("GET", "/token")
    response = await handler(request)

    assert response.status == 200


@pytest.mark.asyncio
async def test_token_rpc_failure():
    svc = TokenRpcService(
        spark=FakeSparkSession(),
        token_manager=FakeTokenManager(fail=True),
        port=9999,
    )

    app = svc.build_app()
    route = next(iter(app.router.routes()))
    handler = route.handler

    request = make_mocked_request("GET", "/token")
    response = await handler(request)

    assert response.status == 500


def test_rpc_service_background_coroutine_returns_coroutine():
    class DummyRpc(RpcService):
        def build_app(self):
            import aiohttp.web
            return aiohttp.web.Application()

    svc = DummyRpc(spark=FakeSparkSession())
    coro = svc.background_coroutine()
    assert hasattr(coro, "__await__")

