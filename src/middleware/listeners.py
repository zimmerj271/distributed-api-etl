# Middleware components that observe but does not change the request or response
import os
import socket
import threading
import time
from datetime import datetime, timezone
from aiohttp import ClientTimeout

from clients.base import RequestExchange
from middleware.pipeline import (
    NEXT_CALL, 
    Middleware, 
    MiddlewareFactory,
    MiddlewareType
)
from transport.base import TransportEngine


@MiddlewareFactory.register(MiddlewareType.LOGGING)
class LoggingMiddleware(Middleware):
    """
    Allows logging from either direction in the middleware pipeline. Logging is
    added to the RequestExchange.metadata.
    """

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        logs = request_exchange.metadata.setdefault("logs", [])
        logs.append(f"-> {request_exchange.context.method.name} {request_exchange.context.url}")

        result = await next_call(request_exchange)

        if result.status_code:
            logs.append(f"<- {result.status_code} {result.context.url}")
        else:
            logs.append(f"<- FAILED {result.context.url}: {result.error_message}")

        return result


@MiddlewareFactory.register(MiddlewareType.TIMING)
class TimingMiddleware(Middleware):
    """
    Measure the elapsed time for the downstream pipeline and store
    the timing info in RequestExchange.metadata["timing"]
    """

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        start = time.monotonic()
        result: RequestExchange = await next_call(request_exchange)
        end = time.monotonic()
        duration = end - start

        timing = dict(result.metadata.get("timing", {}))
        timing["total_seconds"] = float(f"{duration:.2f}")
        result.metadata["timing"] = timing
        return result


@MiddlewareFactory.register(MiddlewareType.WORKER_DIAG)
class WorkerIdentityMiddleware(Middleware):
    """
    Middleware that adds worker/executor identity metadata to each
    RequestExchange metadata. Adds the following:
        - hostname
        - pid
        - thread_id
        - Spark executor ID (if available)
        - Python worker ID 
        - Executor start time
    """

    def __init__(self) -> None:
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.thread_id = threading.get_ident()
        self.executor_id = os.environ.get("SPARK_EXECUTOR_ID")
        self.python_worker_index = os.environ.get("PYSPARK_WORKER")
        self.start_time = datetime.now(timezone.utc).isoformat()

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        identity = request_exchange.metadata.setdefault("executor_identity", {})

        identity.setdefault("hostname", self.hostname)
        identity.setdefault("pid", self.pid)
        identity.setdefault("thread_id", self.thread_id)
        identity.setdefault("executor_id", self.executor_id)
        identity.setdefault("python_worker_index", self.python_worker_index)
        identity.setdefault("worker_process_start_time", self.start_time)

        return await next_call(request_exchange)


class TransportDiagnosticMiddleware(Middleware):
    """
    Inject transport warm-up diagnostics into RequestExchange.
    """

    def __init__(self, transport: TransportEngine) -> None:
        self._transport = transport

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        warmup_error: str | None = getattr(self._transport, "_warmup_error", None)
        warmed_up: bool | None = getattr(self._transport, "_warmed_up", None)
        warmup_timeout: ClientTimeout | None = getattr(self._transport, "_warmup_timeout", None)

        if warmup_timeout is not None:
            warmup_timeout_value = warmup_timeout.total
        else:
            warmup_timeout_value = None

        warmup = request_exchange.metadata.setdefault("connection_warmup", {})
        warmup.setdefault("warmed_up", warmed_up)
        warmup.setdefault("warmup_error", warmup_error)
        warmup.setdefault("warmup_timeout", warmup_timeout_value)

        return await next_call(request_exchange)

