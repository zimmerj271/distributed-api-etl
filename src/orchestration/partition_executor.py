import asyncio
from typing import AsyncGenerator, Callable, Iterable
from pyspark.sql import Row

from core.runtime import WorkerResourceManager
from request_execution.models import RequestContext, RequestExchange
from request_execution.middleware.pipeline import MIDDLEWARE_FUNC
from request_execution.middleware.listeners import TransportDiagnosticMiddleware
from request_execution.middleware.interceptors import ParamInjectorMiddleware
from request_execution.transport.base import TransportEngine
from orchestration.base import PartitionExecutor
from request_execution.executor import RequestExecutor


class ApiPartitionExecutor(PartitionExecutor):
    """
    Concrete and composition-driven partition executor.
    All variability is injected via factories.
    """

    def __init__(
        self,
        *,
        transport_factory: Callable[[], TransportEngine],
        endpoint_factory: Callable[[], RequestContext],
        middleware_factories: list[Callable[[], MIDDLEWARE_FUNC]],
        concurrency_limit: int = 20,
        param_mapping: dict[str, str] | None = None,
        transport_diagnostics: bool = False,
    ) -> None:
        # Serializable factories
        self._transport_factory = transport_factory
        self._endpoint_factory = endpoint_factory
        self._middleware_factories = middleware_factories
        self._concurrency_limit = concurrency_limit
        self._param_mapping = param_mapping
        self._transport_diagnostics = transport_diagnostics

        # Assign type but do not instantiate to ensure it's serializable
        self._resources: WorkerResourceManager | None = None

    def _get_resources(self) -> WorkerResourceManager:
        if self._resources is None:
            self._resources = WorkerResourceManager()
        return self._resources

    def make_map_partitions_fn(
        self,
    ) -> Callable[[Iterable[Row]], list[Row]]:
        async def async_process_partition(
            rows: Iterable[Row],
        ) -> list[Row]:
            middleware_factories = self._middleware_factories

            resources = self._get_resources()

            transport = await resources.get(
                key=TransportEngine, factory=self._transport_factory
            )

            if self._transport_diagnostics:
                middleware_factories.append(
                    lambda: TransportDiagnosticMiddleware(transport)
                )

            if self._param_mapping is not None:
                middleware_factories.insert(0, lambda: ParamInjectorMiddleware())

            executor = RequestExecutor(transport, middleware_factories)
            queue = asyncio.Queue()

            # Use a Producer/Consumer pattern to build a coroutine queue
            # Producer: feeds rows into the queue
            async def producer():
                for row in rows:
                    await queue.put(row)

                # Signal completion to consumers
                for _ in range(self._concurrency_limit):
                    await queue.put(None)  # sentinel per consumer

            # Consumer: pulls from queue and processes row
            async def consumer():
                results = []
                while True:
                    row = await queue.get()
                    if row is None:  # sentinel received
                        break

                    request_context = self._endpoint_factory()
                    request_context._row = row

                    request_exchange = await executor.send(request_context)
                    results.append((row, request_exchange))

                return results

            # Launch producer + consumers
            producer_task = asyncio.create_task(producer())
            consumer_tasks = [
                asyncio.create_task(consumer()) for _ in range(self._concurrency_limit)
            ]

            await producer_task
            all_results = await asyncio.gather(*consumer_tasks)

            # Flatten results from all consumers
            return [
                request_exchange.build_row(row["request_id"])
                for consumer_results in all_results
                for row, request_exchange in consumer_results
            ]

        def sync_process_partition(rows: Iterable[Row]) -> list[Row]:
            return asyncio.run(async_process_partition(rows))

        return sync_process_partition
