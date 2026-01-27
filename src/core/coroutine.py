import asyncio
import logging
import threading
from typing import Callable, Any, Coroutine, TypeVar, Protocol


T = TypeVar('T')

BackgroundCoroutineFactory = Callable[[], Coroutine[Any, Any, None]]


class BackgroundProcess(Protocol):
    """
    A structural design interface for a class that has a process running in 
    a background thread by AsyncBackgroundService.
    """
    def background_coroutine(self) -> Coroutine[Any, Any, None]: ...


class AsyncBackgroundService:
    """
    Utility for running a single long-lived async coroutine in its own dedicated
    thread and asyncio event loop.

    AsyncBackgroundService decouples "what work should run in the background"
    from "how to host that work." It accepts a factory callable that returns
    an async coroutine (e.g., RpcService.background_coroutine or
    DriverTokenManager.background_coroutine) and takes care of:

      • Creating a dedicated thread.
      • Creating and configuring an asyncio event loop in that thread.
      • Scheduling the provided coroutine as a task on that loop.
      • Keeping the loop alive via `run_forever()` until shutdown is requested.
      • Providing a synchronous start()/stop() lifecycle that can be called
        from regular (non-async) driver code.

    Typical usage:
      • Construct AsyncBackgroundService with a background coroutine factory:
            service = AsyncBackgroundService(rpc_service.background_coroutine)
      • Call `service.start()` to spin up the thread and event loop, and to
        schedule the coroutine.
      • Call `service.stop()` to cancel the task, stop the loop, and join the
        thread.

    This abstraction is especially important in a Spark/Databricks environment,
    where the driver-side code is generally synchronous, but we still need
    asynchronous services (like an aiohttp RPC listener or a token refresh
    loop) to run independently for the duration of the job. By centralizing
    event-loop and thread management here, the rest of the codebase can treat
    these background services as simple start/stop components.
    """
    
    def __init__(self, background_fn: BackgroundCoroutineFactory) -> None:
        self._background_fn = background_fn
        self._service_name = background_fn.__qualname__.split(".")[0]
        self._logger = logging.getLogger(f"{self.__class__.__name__}[{self._service_name}]")
        
        # Background thread attributes
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._task: asyncio.Task[None] | None = None
        self._ready_event = threading.Event()
    
    def start(self) -> None:
        """
        Start an idempotent background event loop thread.
        """
        if self._thread is not None and self._thread.is_alive():
            self._logger.warning(f"Already running")
            return

        self._ready_event.clear()
        self._thread = threading.Thread(
            target=self._run_event_loop,
            name=f"{self._service_name}-loop-thread",
            daemon=True
        )
        self._thread.start()

        # Wait for initialization
        if not self._ready_event.wait(timeout=10):
            raise TimeoutError(f"Failed to start within 10 seconds")
        
        self._logger.info(f"Background service started")

    def _run_event_loop(self) -> None:
        """
        The thread target: create an asyncio event loop, run the background coroutine,
        and keep the loop alive until stop() is called.
        """
        try:
            # create an event loop
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(self._loop)

            # Create the coroutine and schedule it as a task 
            coroutine: Coroutine[Any, Any, None] = self._background_fn()
            self._task = loop.create_task(coroutine)

            # Signal ready
            self._ready_event.set()
            
            # Run loop
            loop.run_forever()

            # After loop stops, give pending task a chance to finish
            if self._task is not None and not self._task.done():
                loop.run_until_complete(
                    asyncio.gather(self._task, return_exceptions=True)
                )

        except Exception as e:
            self._logger.error(f"Background runtime error: {e}")
            self._ready_event.set()

        finally:
            if self._loop is not None:
                self._loop.close()
                self._loop = None

    async def _shutdown_task_async(self) -> None:
        """
        Async shutdown: cancle the background task (if running) and stop the loop.
        """
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task 
            except asyncio.CancelledError:
                self._logger.info(f"Background task cancelled")

        loop = asyncio.get_running_loop()
        loop.stop()
    
    def stop(self) -> None:
        """Stop the background coroutine and shut down the loop/thread."""
        if self._loop is None:
            self._logger.warning(f"Not running")
            return
        
        self._logger.info(f"Stopping...")

        def _cancel_and_stop() -> None:
            """Cancel thread and stop loop"""
            if self._task is not None and not self._task.done():
                self._task.cancel()
            if self._loop is not None and self._loop.is_running():
                self._loop.stop()

        # Schedule cancellation and loop.stop in the event loop's thread
        try:
            self._loop.call_soon_threadsafe(_cancel_and_stop)
        except Exception as e:
            self._logger.error(f"Error during shutdown: {e}")

        # Wait for thread to exit
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                self._logger.warning(
                    f"Thread did not stop gracefully"
                )
            else:
                self._logger.info(f"Stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._thread is not None and self._thread.is_alive()
