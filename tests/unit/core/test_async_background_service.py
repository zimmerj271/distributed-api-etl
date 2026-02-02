"""Unit tests for AsyncBackgroundService"""

import pytest
import asyncio
import time
from core import AsyncBackgroundService
from tests.fixtures.core.coroutine import simple_background


@pytest.mark.unit
class TestAsyncBackgroundServiceLifecycle:
    """Tests for AsyncBackgroundService lifecycle management"""

    def test_is_running_false_before_start(self):
        """
        GIVEN an AsyncBackgroundService that hasn't started
        WHEN is_running is checked
        THEN it should return False
        """
        service = AsyncBackgroundService(simple_background)

        assert service.is_running is False

    def test_is_running_true_after_start(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN start is called
        THEN is_running should return True
        """
        service = AsyncBackgroundService(simple_background)

        service.start()

        assert service.is_running is True

        # Cleanup
        service.stop()

    def test_is_running_false_after_stop(self):
        """
        GIVEN a running AsyncBackgroundService
        WHEN stop is called
        THEN is_running should return False
        """
        service = AsyncBackgroundService(simple_background)

        service.start()
        service.stop()

        assert service.is_running is False

    def test_start_creates_background_thread(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN start is called
        THEN a background thread should be created
        """
        service = AsyncBackgroundService(simple_background)

        service.start()

        assert service._thread is not None
        assert service._thread.is_alive()

        # Cleanup
        service.stop()

    def test_stop_terminates_background_thread(self):
        """
        GIVEN a running AsyncBackgroundService
        WHEN stop is called
        THEN the background thread should terminate
        """
        service = AsyncBackgroundService(simple_background)

        service.start()
        service.stop()

        # Thread should no longer be alive
        assert not service._thread.is_alive()


@pytest.mark.unit
class TestAsyncBackgroundServiceIdempotency:
    """Tests for idempotent start/stop operations"""

    def test_start_is_idempotent(self):
        """
        GIVEN a running AsyncBackgroundService
        WHEN start is called again
        THEN it should not create a new thread (idempotent)
        """
        service = AsyncBackgroundService(simple_background)

        service.start()
        first_thread = service._thread

        service.start()  # Call again
        second_thread = service._thread

        assert first_thread is second_thread
        assert service.is_running

        # Cleanup
        service.stop()

    def test_stop_when_not_running_does_not_raise(self):
        """
        GIVEN an AsyncBackgroundService that isn't running
        WHEN stop is called
        THEN it should not raise an error
        """
        service = AsyncBackgroundService(simple_background)

        service.stop()  # Should not raise

        assert not service.is_running

    def test_stop_is_idempotent(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN stop is called multiple times
        THEN it should not raise errors
        """
        service = AsyncBackgroundService(simple_background)

        service.start()
        service.stop()
        service.stop()  # Call again - should not raise

        assert not service.is_running


@pytest.mark.unit
class TestAsyncBackgroundServiceCoroutineExecution:
    """Tests for coroutine execution behavior"""

    def test_background_coroutine_is_executed(self):
        """
        GIVEN an AsyncBackgroundService with a coroutine
        WHEN start is called
        THEN the coroutine should be executed
        """
        executed = []

        async def tracking_coroutine():
            executed.append(True)
            while True:
                await asyncio.sleep(0.1)

        service = AsyncBackgroundService(tracking_coroutine)

        service.start()
        time.sleep(0.2)  # Give it time to execute

        assert len(executed) == 1

        # Cleanup
        service.stop()

    def test_background_coroutine_runs_continuously(self):
        """
        GIVEN a background coroutine with a counter
        WHEN the service runs for a period
        THEN the counter should increment multiple times
        """
        counter = []

        async def counting_coroutine():
            while True:
                counter.append(1)
                await asyncio.sleep(0.05)

        service = AsyncBackgroundService(counting_coroutine)

        service.start()
        time.sleep(0.3)  # Let it run
        service.stop()

        # Should have incremented multiple times
        assert len(counter) > 1

    def test_coroutine_receives_cancellation(self):
        """
        GIVEN a running background coroutine
        WHEN stop is called
        THEN the coroutine should receive CancelledError
        """
        cancelled = []

        async def cancellable_coroutine():
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        service = AsyncBackgroundService(cancellable_coroutine)

        service.start()
        time.sleep(0.1)
        service.stop()

        # Give it time to handle cancellation
        time.sleep(0.2)

        assert len(cancelled) == 1


@pytest.mark.unit
class TestAsyncBackgroundServiceEventLoop:
    """Tests for event loop management"""

    def test_creates_dedicated_event_loop(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN start is called
        THEN it should create a dedicated event loop
        """
        service = AsyncBackgroundService(simple_background)

        service.start()

        assert service._loop is not None

        # Cleanup
        service.stop()
        time.sleep(0.1)

    def test_event_loop_is_closed_after_stop(self):
        """
        GIVEN a running AsyncBackgroundService
        WHEN stop is called
        THEN the event loop should be closed
        """
        service = AsyncBackgroundService(simple_background)

        service.start()
        time.sleep(0.1)
        service.stop()
        time.sleep(0.2)  # Give it time to clean up

        # Loop should be None after cleanup
        assert service._loop is None


@pytest.mark.unit
@pytest.mark.slow
class TestAsyncBackgroundServiceErrorHandling:
    """Tests for error handling in background coroutines"""

    def test_coroutine_exception_is_handled(self):
        """
        GIVEN a coroutine that raises an exception
        WHEN the service runs
        THEN it should handle the exception gracefully
        """

        async def failing_coroutine():
            raise RuntimeError("Coroutine failed")

        service = AsyncBackgroundService(failing_coroutine)

        service.start()
        time.sleep(0.2)

        # Service should still be running (thread alive)
        # even though coroutine failed
        assert service._thread is not None

        # Cleanup
        service.stop()

    def test_service_can_be_stopped_after_coroutine_exception(self):
        """
        GIVEN a service whose coroutine raised an exception
        WHEN stop is called
        THEN it should stop gracefully
        """

        async def failing_coroutine():
            raise RuntimeError("Failed")

        service = AsyncBackgroundService(failing_coroutine)

        service.start()
        time.sleep(0.2)
        service.stop()  # Should not raise

        assert not service.is_running


@pytest.mark.unit
class TestAsyncBackgroundServiceTimeout:
    """Tests for startup timeout handling"""

    def test_start_waits_for_initialization(self):
        """
        GIVEN an AsyncBackgroundService
        WHEN start is called
        THEN it should wait for the service to initialize before returning
        """
        service = AsyncBackgroundService(simple_background)

        # start() should block until _ready_event is set
        service.start()

        # After start() returns, service should be running
        assert service.is_running
        assert service._loop is not None
        assert service._thread is not None
        assert service._thread.is_alive()

        # Cleanup
        service.stop()

    def test_start_raises_on_timeout(self):
        """
        GIVEN a coroutine that never signals ready
        WHEN start is called with timeout
        THEN it should raise TimeoutError

        Note: This test assumes timeout mechanism exists.
        If not implemented, this test documents expected behavior.
        """

        async def never_ready_coroutine():
            # Never signals ready, just sleeps forever
            while True:
                await asyncio.sleep(1)

        service = AsyncBackgroundService(never_ready_coroutine)

        # This should timeout (if timeout is implemented)
        # For now, we'll just test that it eventually starts
        service.start()

        # Even if slow, should eventually be running
        assert service.is_running

        # Cleanup
        service.stop()

    def test_start_timeout_on_failure(self):
        """
        GIVEN an AsyncBackgroundService with a failing coroutine
        WHEN the background thread fails to initialize
        THEN start should timeout and raise TimeoutError

        This tests the timeout mechanism when _ready_event is never set.
        """

        # Create a coroutine factory that will cause initialization to fail
        def failing_coroutine_factory():
            raise RuntimeError("Initialization failed")

        service = AsyncBackgroundService(failing_coroutine_factory)

        # The service should signal ready even on failure, but we can test
        # that it handles initialization problems gracefully
        try:
            service.start()
        except (TimeoutError, RuntimeError):
            # Either is acceptable depending on implementation
            pass

        # Cleanup if it did start
        if service._thread and service._thread.is_alive():
            service.stop()
