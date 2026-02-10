import asyncio
from typing import AsyncGenerator, Callable, Iterable 
from pyspark.sql import Row

from core.runtime import WorkerResourceManager
from request_execution.models import RequestContext, RequestExchange
from request_execution.middleware.pipeline import MIDDLEWARE_FUNC
from request_execution.middleware.listeners import TransportDiagnosticMiddleware
from request_execution.middleware.interceptors import ParamInjectorMiddleware
from request_execution.transport.base import TransportEngine
from pipeline.base import PartitionExecutor
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
    ) -> None:

        # Serializable factories
        self._transport_factory = transport_factory
        self._endpoint_factory = endpoint_factory
        self._middleware_factories = middleware_factories

        # Assign type but do not instantiate to ensure it's serializable
        self._resources: WorkerResourceManager | None = None

    def  _get_resources(self) -> WorkerResourceManager:
        if self._resources is None:
            self._resources = WorkerResourceManager()
        return self._resources

    def make_map_partitions_fn(
        self, 
    ) -> Callable[[Iterable[Row]], Iterable[Row]]:
        
        async def async_process_partition(
            rows: Iterable[Row]
        ) -> AsyncGenerator[Row, None]:

            resources = self._get_resources()

            transport = await resources.get(
                key=TransportEngine,
                factory=self._transport_factory
            )

            client = RequestExecutor(transport)
            request_context = self._endpoint_factory()

            if request_context.param_mapping is not None:
                client.add_middleware(
                    ParamInjectorMiddleware(request_context.param_mapping)
                )

            # Late-binding diagnostics -- requires transport
            client.add_middleware(
                TransportDiagnosticMiddleware(transport)
            )

            for factory in self._middleware_factories:
                client.add_middleware(factory())


            for row in rows:
                request_context._row = row
                rr: RequestExchange = await client.send(request_context)
                yield rr.build_row(row["request_id"])

        def sync_process_partition(
            rows: Iterable[Row]
        ) -> Iterable[Row]:

            async def collect():
                return [r async for r in async_process_partition(rows)]

            return iter(asyncio.run(collect()))

        return sync_process_partition
