"""Unit tests for ProcessScope resource management"""

import pytest
import asyncio
from core.runtime import ProcessScope
from core.exceptions import MultipleCleanupFailures
from tests.fixtures.core.runtime import DummyAsyncResource


@pytest.mark.unit
@pytest.mark.asyncio
class TestProcessScopeSingletonBehavior:
    """Tests for ProcessScope singleton-per-type behavior"""

    async def test_returns_same_instance_for_same_type(self):
        """
        GIVEN a ProcessScope
        WHEN get is called multiple times for the same type
        THEN it should return the same instance
        """
        scope = ProcessScope()
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return DummyAsyncResource()

        r1 = await scope.get(DummyAsyncResource, factory)
        r2 = await scope.get(DummyAsyncResource, factory)
        r3 = await scope.get(DummyAsyncResource, factory)

        assert r1 is r2 is r3
        assert call_count == 1

    async def test_different_types_get_different_instances(self):
        """
        GIVEN a ProcessScope
        WHEN get is called for different types
        THEN each type should get its own instance
        """

        class ResourceA(DummyAsyncResource):
            pass

        class ResourceB(DummyAsyncResource):
            pass

        scope = ProcessScope()

        a = await scope.get(ResourceA, lambda: ResourceA())
        b = await scope.get(ResourceB, lambda: ResourceB())

        assert a is not b
        assert isinstance(a, ResourceA)
        assert isinstance(b, ResourceB)


@pytest.mark.unit
@pytest.mark.asyncio
class TestProcessScopeResourceLifecycle:
    """Tests for async resource lifecycle management"""

    async def test_calls_aenter_on_creation(self):
        """
        GIVEN a ProcessScope
        WHEN a resource is created
        THEN __aenter__ should be called
        """
        scope = ProcessScope()

        def factory():
            return DummyAsyncResource()

        resource = await scope.get(DummyAsyncResource, factory)

        assert resource.entered is True

    async def test_calls_aexit_on_close(self):
        """
        GIVEN a ProcessScope with a managed resource
        WHEN close is called
        THEN __aexit__ should be called on the resource
        """
        scope = ProcessScope()

        def factory():
            return DummyAsyncResource()

        resource = await scope.get(DummyAsyncResource, factory)
        assert resource.exited is False

        await scope.close()

        assert resource.exited is True

    async def test_close_handles_multiple_resources(self):
        """
        GIVEN a ProcessScope managing multiple resources
        WHEN close is called
        THEN all resources should be cleaned up
        """

        class ResourceA(DummyAsyncResource):
            pass

        class ResourceB(DummyAsyncResource):
            pass

        scope = ProcessScope()

        a = await scope.get(ResourceA, lambda: ResourceA())
        b = await scope.get(ResourceB, lambda: ResourceB())

        await scope.close()

        assert a.exited is True
        assert b.exited is True

    async def test_close_is_idempotent(self):
        """
        GIVEN a closed ProcessScope
        WHEN close is called again
        THEN it should not raise errors
        """
        scope = ProcessScope()

        resource = await scope.get(DummyAsyncResource, lambda: DummyAsyncResource())

        await scope.close()
        await scope.close()  # Should not raise

        assert resource.exited is True


@pytest.mark.unit
@pytest.mark.asyncio
class TestProcessScopeConcurrency:
    """Tests for ProcessScope concurrent access handling"""

    async def test_concurrent_gets_return_same_instance(self):
        """
        GIVEN a ProcessScope
        WHEN multiple coroutines call get concurrently for same type
        THEN only one instance should be created (lock works)
        """
        scope = ProcessScope()
        call_count = 0

        def factory():
            nonlocal call_count
            call_count += 1
            return DummyAsyncResource()

        # Launch 10 concurrent gets
        resources = await asyncio.gather(
            *[scope.get(DummyAsyncResource, factory) for _ in range(10)]
        )

        # All should be the same instance
        assert all(r is resources[0] for r in resources)
        # Factory should only be called once
        assert call_count == 1

    async def test_concurrent_gets_for_different_types(self):
        """
        GIVEN a ProcessScope
        WHEN concurrent gets for different types
        THEN each type should get its own instance
        """

        class ResourceA(DummyAsyncResource):
            pass

        class ResourceB(DummyAsyncResource):
            pass

        scope = ProcessScope()

        # Get both types concurrently
        a, b = await asyncio.gather(
            scope.get(ResourceA, lambda: ResourceA()),
            scope.get(ResourceB, lambda: ResourceB()),
        )

        assert a is not b
        assert isinstance(a, ResourceA)
        assert isinstance(b, ResourceB)


@pytest.mark.unit
@pytest.mark.asyncio
class TestProcessScopeEdgeCases:
    """Tests for edge cases and error handling"""

    async def test_get_after_close_still_works(self):
        """
        GIVEN a closed ProcessScope
        WHEN get is called
        THEN it should still work (creates new resource)
        """
        scope = ProcessScope()

        r1 = await scope.get(DummyAsyncResource, lambda: DummyAsyncResource())
        await scope.close()

        # This creates a new resource (scope is empty after close)
        r2 = await scope.get(DummyAsyncResource, lambda: DummyAsyncResource())

        assert r1 is not r2
        assert r2.entered is True

    async def test_factory_exception_propagates(self):
        """
        GIVEN a factory that raises an exception
        WHEN get is called
        THEN the exception should propagate
        """
        scope = ProcessScope()

        def failing_factory():
            raise RuntimeError("Factory failed")

        with pytest.raises(RuntimeError, match="Factory failed"):
            await scope.get(DummyAsyncResource, failing_factory)

    async def test_aexit_exception_is_raised(self):
        """
        GIVEN a resource whose __aexit__ raises an exception
        WHEN close is called
        THEN the exception should be raised (or collected)
        """

        class FailingResource(DummyAsyncResource):
            async def __aexit__(self, exc_type, exc, exc_tb):
                self.exited = True
                raise RuntimeError("Exit failed")

        scope = ProcessScope()
        failing = await scope.get(FailingResource, lambda: FailingResource())

        # Should raise the exception from __aexit__
        with pytest.raises(RuntimeError, match="Exit failed"):
            await scope.close()

        assert failing.exited is True

    async def test_aexit_exception_does_not_prevent_other_cleanups(self):
        """
        GIVEN multiple resources where one __aexit__ raises
        WHEN close is called
        THEN all resources should still be cleaned up (exception handling)

        NOTE: This test assumes ProcessScope.close() has proper exception handling.
        If not implemented, this documents the expected behavior.
        """

        class FailingResource(DummyAsyncResource):
            async def __aexit__(self, exc_type, exc, exc_tb):
                self.exited = True
                raise RuntimeError("Exit failed")

        class NormalResource(DummyAsyncResource):
            pass

        scope = ProcessScope()

        # Important: Order matters - add failing resource first
        # to ensure it doesn't prevent cleanup of resources added after
        failing = await scope.get(FailingResource, lambda: FailingResource())
        normal = await scope.get(NormalResource, lambda: NormalResource())

        # Close should handle the exception but still clean up other resources
        try:
            await scope.close()
        except (RuntimeError, MultipleCleanupFailures):
            pass  # Expected

        # Both should be exited
        assert failing.exited is True
        assert normal.exited is True
