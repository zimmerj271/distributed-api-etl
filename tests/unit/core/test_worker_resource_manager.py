"""Unit tests for WorkerResourceManager"""
import pytest
import asyncio
from core import WorkerResourceManager
from tests.fixtures.core import DummyAsyncResource


@pytest.mark.unit
@pytest.mark.asyncio
class TestWorkerResourceManagerBasicBehavior:
    """Tests for basic WorkerResourceManager functionality"""
    
    async def test_get_creates_and_enters_resource(self):
        """
        GIVEN a WorkerResourceManager
        WHEN get is called for a resource type
        THEN it should create and enter the resource
        """
        mgr = WorkerResourceManager()

        def factory():
            return DummyAsyncResource()

        resource = await mgr.get(DummyAsyncResource, factory)

        assert resource.entered is True
        assert resource.exited is False
    
    async def test_get_returns_same_instance_on_multiple_calls(self):
        """
        GIVEN a WorkerResourceManager
        WHEN get is called multiple times for same type
        THEN it should return the same instance
        """
        mgr = WorkerResourceManager()
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return DummyAsyncResource()

        r1 = await mgr.get(DummyAsyncResource, factory)
        r2 = await mgr.get(DummyAsyncResource, factory)

        assert r1 is r2
        assert call_count == 1
    
    async def test_close_exits_all_resources(self):
        """
        GIVEN a WorkerResourceManager with managed resources
        WHEN close is called
        THEN all resources should be exited
        """
        mgr = WorkerResourceManager()

        resource = await mgr.get(DummyAsyncResource, lambda: DummyAsyncResource())
        
        await mgr.close()

        assert resource.exited is True


@pytest.mark.unit
@pytest.mark.asyncio
class TestWorkerResourceManagerClosedState:
    """Tests for WorkerResourceManager closed state handling"""
    
    async def test_get_after_close_raises_error(self):
        """
        GIVEN a closed WorkerResourceManager
        WHEN get is called
        THEN it should raise RuntimeError
        """
        mgr = WorkerResourceManager()
        await mgr.close()

        def factory():
            return DummyAsyncResource()

        with pytest.raises(RuntimeError, match="closed"):
            await mgr.get(DummyAsyncResource, factory)
    
    async def test_close_is_idempotent(self):
        """
        GIVEN a WorkerResourceManager
        WHEN close is called multiple times
        THEN it should not raise errors
        """
        mgr = WorkerResourceManager()
        
        resource = await mgr.get(DummyAsyncResource, lambda: DummyAsyncResource())
        
        await mgr.close()
        await mgr.close()  # Should not raise
        
        assert resource.exited is True
    
    async def test_closed_state_prevents_new_resources(self):
        """
        GIVEN a closed WorkerResourceManager
        WHEN attempting to get different resource types
        THEN all should raise RuntimeError
        """
        class ResourceA(DummyAsyncResource):
            pass
        
        class ResourceB(DummyAsyncResource):
            pass
        
        mgr = WorkerResourceManager()
        await mgr.close()
        
        with pytest.raises(RuntimeError, match="closed"):
            await mgr.get(ResourceA, lambda: ResourceA())
        
        with pytest.raises(RuntimeError, match="closed"):
            await mgr.get(ResourceB, lambda: ResourceB())


@pytest.mark.unit
@pytest.mark.asyncio
class TestWorkerResourceManagerMultipleTypes:
    """Tests for managing multiple resource types"""
    
    async def test_manages_multiple_resource_types_independently(self):
        """
        GIVEN a WorkerResourceManager
        WHEN get is called for different resource types
        THEN each should get its own instance
        """
        class ResourceA(DummyAsyncResource):
            pass
        
        class ResourceB(DummyAsyncResource):
            pass
        
        mgr = WorkerResourceManager()
        
        a = await mgr.get(ResourceA, lambda: ResourceA())
        b = await mgr.get(ResourceB, lambda: ResourceB())
        
        assert a is not b
        assert isinstance(a, ResourceA)
        assert isinstance(b, ResourceB)
    
    async def test_close_cleans_up_all_resource_types(self):
        """
        GIVEN a WorkerResourceManager with multiple resource types
        WHEN close is called
        THEN all resources should be cleaned up
        """
        class ResourceA(DummyAsyncResource):
            pass
        
        class ResourceB(DummyAsyncResource):
            pass
        
        mgr = WorkerResourceManager()
        
        a = await mgr.get(ResourceA, lambda: ResourceA())
        b = await mgr.get(ResourceB, lambda: ResourceB())
        
        await mgr.close()
        
        assert a.exited is True
        assert b.exited is True


@pytest.mark.unit
@pytest.mark.asyncio
class TestWorkerResourceManagerConcurrency:
    """Tests for concurrent access to WorkerResourceManager"""
    
    async def test_concurrent_get_returns_same_instance(self):
        """
        GIVEN a WorkerResourceManager
        WHEN multiple coroutines call get concurrently
        THEN only one instance should be created
        """
        mgr = WorkerResourceManager()
        call_count = 0
        
        def factory():
            nonlocal call_count
            call_count += 1
            return DummyAsyncResource()
        
        # Launch 10 concurrent gets
        resources = await asyncio.gather(*[
            mgr.get(DummyAsyncResource, factory)
            for _ in range(10)
        ])
        
        # All should be same instance
        assert all(r is resources[0] for r in resources)
        # Factory called only once
        assert call_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
class TestWorkerResourceManagerErrorHandling:
    """Tests for error handling in WorkerResourceManager"""
    
    async def test_factory_exception_propagates(self):
        """
        GIVEN a factory that raises an exception
        WHEN get is called
        THEN the exception should propagate
        """
        mgr = WorkerResourceManager()
        
        def failing_factory():
            raise ValueError("Factory error")
        
        with pytest.raises(ValueError, match="Factory error"):
            await mgr.get(DummyAsyncResource, failing_factory)
    
    async def test_failed_factory_does_not_cache_error(self):
        """
        GIVEN a factory that fails then succeeds
        WHEN get is called after failure
        THEN it should retry the factory
        """
        mgr = WorkerResourceManager()
        attempt = 0
        
        def flakey_factory():
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise RuntimeError("First attempt fails")
            return DummyAsyncResource()
        
        # First call fails
        with pytest.raises(RuntimeError):
            await mgr.get(DummyAsyncResource, flakey_factory)
        
        # Second call should succeed
        resource = await mgr.get(DummyAsyncResource, flakey_factory)
        assert resource is not None
        assert attempt == 2
