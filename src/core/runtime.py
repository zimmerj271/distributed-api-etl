import asyncio
from types import TracebackType
from typing import Callable, Any, TypeVar, Generic, Protocol, Self


class AsyncProcess(Protocol):

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self, 
        exc_type: BaseException | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None,
    ) -> None:
        ...


T = TypeVar("T", bound=AsyncProcess)


class ProcessScope(Generic[T]):
    """
    ProcessScope

    ProcessScope models a process-level ownership boundary for asynchronous
    resources. It provides a structured way to create, cache, and shut down
    async-managed objects whose lifetime must span multiple logical execution
    units (e.g., Spark partitions) within the same Python process.

    In contrast to request-scoped or partition-scoped lifetimes, ProcessScope
    ensures that resources such as HTTP transports, connection pools, and
    client sessions are:

      - Initialized exactly once per process.
      - Reused safely across multiple callers.
      - Shut down through a single, well-defined async close path.

    ProcessScope does NOT define *when* a process starts or ends. Instead, it
    assumes that an external owner (such as WorkerResourceManager) controls
    process lifetime and invokes ProcessScope.close() at the appropriate time.

    Resource Model
    --------------
    Resources managed by ProcessScope must conform to an async lifecycle
    contract (e.g., implement `__aenter__` / `__aexit__`). ProcessScope enforces
    the following invariants:

      - Resource creation is lazy and keyed by type.
      - At most one instance of a given resource type exists per process.
      - Async initialization (`__aenter__`) is invoked exactly once.
      - Concurrent callers requesting the same resource are synchronized
        to prevent duplicate creation.

    Internally, ProcessScope uses asyncio locks to ensure correctness under
    concurrent access within the same event loop.

    Shutdown Semantics
    ------------------
    ProcessScope provides an explicit async `close()` method that:

      - Invokes `__aexit__` on all managed resources.
      - Releases references to owned resources.
      - Does not assume ownership of the event loop itself.

    ProcessScope makes no guarantees about when `close()` is called. It is the
    responsibility of the owning runtime (e.g., WorkerResourceManager) to
    ensure that `close()` is invoked during graceful process shutdown.

    Scope and Responsibilities
    --------------------------
    - ProcessScope is intentionally unaware of Spark, partitions, or execution
      models.
    - It does not perform logging, metrics, retries, or error handling beyond
      resource lifecycle coordination.
    - It does not create or manage event loops.
    - It does not expose partial shutdown or per-resource teardown semantics.

    In short, ProcessScope is a minimal, deterministic abstraction for managing
    the lifecycle of async resources at process scope, delegating all decisions
    about *when* that scope begins or ends to its owner.
    """

    def __init__(self) -> None:
        self._resources: dict[type, Any] = {}
        self._locks: dict[type, asyncio.Lock] = {}

    async def get(
        self,
        key: type[T],
        factory: Callable[[], T]
    ) -> T:
        if key in self._resources:
            return self._resources[key]

        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key not in self._resources:
                resource = factory()
                if hasattr(resource, "__aenter__"):
                    await resource.__aenter__()
                self._resources[key] = resource

        return self._resources[key]

    async def close(self) -> None:
        exceptions = []

        for resource in self._resources.values():
            if hasattr(resource, "__aexit__"):
                try:
                    await resource.__aexit__(None, None, None)
                except Exception as e:
                    exceptions.append(e)

        self._resources.clear()

        if exceptions:
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                raise ExceptionGroup("Multiple cleanup failures", exceptions)


class WorkerResourceManager:
    """
    WorkerResourceManager

    WorkerResourceManager is a worker-local owner of long-lived asynchronous
    resources in a Spark Python worker process. Its primary responsibility
    is to ensure that async resources created on the worker (such as HTTP
    transports, client sessions, and connection pools) are:

      1. Created lazily *after* the Spark driver has serialized and shipped
         the task closure to the worker.
      2. Reused safely across multiple partition executions within the same
         Python worker process.
      3. Shut down gracefully on best-effort worker process termination.

    This class exists specifically to bridge Spark's execution model—which
    provides no explicit Python-level worker shutdown hooks—with Python's
    asynchronous resource lifecycle requirements.

    Relationship to ProcessScope
    -----------------------------
    WorkerResourceManager delegates actual resource lifecycle management to
    ProcessScope. ProcessScope models a process-level ownership boundary for
    async resources by:

      - Creating resources once per process.
      - Ensuring async initialization (`__aenter__`) occurs exactly once.
      - Tracking all owned resources.
      - Providing a single async shutdown path (`close()`).

    WorkerResourceManager injects ProcessScope and supplies it with:
      - A dedicated asyncio event loop owned by the worker.
      - A synchronous facade (`get(...)`) suitable for Spark's
        `mapPartitions` execution model.

    WorkerResourceManager itself does NOT implement business logic, request
    handling, retries, metrics, or observability. Those concerns are handled
    elsewhere (e.g., middleware). Its role is intentionally narrow and
    operational: resource ownership and shutdown.

    Shutdown Semantics
    ------------------
    Spark does not provide a reliable Python-level callback when a worker
    process is about to exit. To compensate, WorkerResourceManager registers
    a best-effort shutdown hook using `atexit`. When the Python interpreter
    terminates normally, this hook:

      - Invokes ProcessScope.close(), allowing async resources to release
        sockets, file descriptors, and other external resources.
      - Closes the worker-owned asyncio event loop.

    This mechanism is best-effort and will not run in cases where the worker
    process is forcibly terminated (e.g., SIGKILL). However, it is the most
    reliable and portable cleanup strategy available in pure Python when
    running under Spark.

    Scope and Guarantees
    --------------------
    - Exactly one WorkerResourceManager is intended to exist per Python worker
      process.
    - Resources are never created on the Spark driver.
    - Resources are never shut down per partition.
    - Resource reuse across partitions is intentional and required to
      amortize connection warmup and pooling costs.
    - Observability and diagnostics are explicitly out of scope for this
      class and should be implemented via middleware or downstream dataflow.
    """

    def __init__(self) -> None:
        self._scope = ProcessScope()
        self._closed = False

    async def get(self, key: type[T], factory: Callable[[], T]) -> T:
        if self._closed:
            raise RuntimeError("WorkerResourceManager is closed")

        return await self._scope.get(key, factory)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        await self._scope.close()
