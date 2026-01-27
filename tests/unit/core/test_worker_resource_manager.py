import pytest
from core.runtime import WorkerResourceManager
from tests.fixtures.core.runtime import DummyAsyncResource


@pytest.mark.asyncio
async def test_worker_resource_manager_get_and_close():
    mgr = WorkerResourceManager()

    def factory() -> DummyAsyncResource:
        return DummyAsyncResource()

    resource = await mgr.get(DummyAsyncResource, factory)

    assert resource.entered is True
    await mgr.close()
    assert resource.exited is True


@pytest.mark.asyncio
async def test_worker_resource_manager_closed_raises():
    mgr = WorkerResourceManager()
    await mgr.close()

    def factory() -> DummyAsyncResource:
        return DummyAsyncResource()

    with pytest.raises(RuntimeError):
        await mgr.get(DummyAsyncResource, factory)

