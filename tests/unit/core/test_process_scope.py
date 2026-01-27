import pytest
from core.runtime import ProcessScope
from tests.fixtures.core.runtime import DummyAsyncResource


@pytest.mark.asyncio
async def test_process_scope_singleton_per_key():
    scope = ProcessScope()
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return DummyAsyncResource()

    r1 = await scope.get(DummyAsyncResource, factory)
    r2 = await scope.get(DummyAsyncResource, factory)

    assert r1 is r2
    assert calls == 1


@pytest.mark.asyncio
async def test_process_scope_enters_and_exits():
    scope = ProcessScope()

    def factory():
        return DummyAsyncResource()

    resource = await scope.get(DummyAsyncResource, factory)
    assert resource.entered is True

    await scope.close()
    assert resource.exited is True

