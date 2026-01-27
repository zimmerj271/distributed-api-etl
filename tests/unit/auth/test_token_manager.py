import pytest
from auth.token.token_manager import TokenManager, DriverTokenManager
from tests.fixtures.auth_token import (
    FakeTokenProvider,
    valid_token,
    expired_token,
)
from tests.fixtures.auth_token import FakeTokenProvider, valid_token


@pytest.mark.asyncio
async def test_token_manager_caches_token():
    provider = FakeTokenProvider(valid_token())
    mgr = TokenManager(provider)

    t1 = await mgr.get_token()
    t2 = await mgr.get_token()

    assert t1 is t2
    assert provider.calls == 1


@pytest.mark.asyncio
async def test_token_manager_refreshes_expired():
    provider = FakeTokenProvider(expired_token())
    mgr = TokenManager(provider, refresh_margin=0)

    await mgr.get_token()
    await mgr.get_token()

    assert provider.calls == 2


@pytest.mark.asyncio
async def test_token_manager_force_refresh():
    provider = FakeTokenProvider(valid_token())
    mgr = TokenManager(provider)

    await mgr.get_token()
    await mgr.force_refresh()

    assert provider.calls == 2


@pytest.mark.asyncio
async def test_token_manager_invalidate():
    provider = FakeTokenProvider(valid_token())
    mgr = TokenManager(provider)

    await mgr.get_token()
    mgr.invalidate()
    await mgr.get_token()

    assert provider.calls == 2


def test_driver_token_manager_background_coroutine():
    provider = FakeTokenProvider(valid_token())
    mgr = DriverTokenManager(provider)

    coro = mgr.background_coroutine()
    assert callable(coro)

