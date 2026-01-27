from enum import Enum
from typing import Awaitable, Callable, Protocol

from clients.base import RequestExchange
from core.abstract_factory import TypeAbstractFactory


NEXT_CALL = Callable[[RequestExchange], Awaitable[RequestExchange]]
MIDDLEWARE_FUNC = Callable[[RequestExchange, NEXT_CALL], Awaitable[RequestExchange]]


class MiddlewareType(str, Enum):
    BEARER = "bearer"
    HEADER = "header"
    RETRY = "retry"
    LOGGING = "logging"
    TIMING = "timing"
    JSON_BODY = "json_body"
    WORKER_DIAG = "worker_diag"


class Middleware(Protocol):
    """
    Middleware trasnforms RequestContext before it reaches transport. The purpose
    of these objects are for data transformation: add headers, validate, serialize.
    Uses a chain pattern: each middleware receive context and "next" function in the chain.
    It can transform context, then call next or short-circuit.
    """

    async def __call__(
        self,
        request_exchange: RequestExchange,
        next_call: NEXT_CALL 
    ) -> RequestExchange:
        """
        Transform context and pass to the next middleware.
        Args:
            context: Current request context.
            next_call: Function to call next middleware in the chain.

        Returns:
            Transformed RequestContext
        """
        ...


class MiddlewareFactory(TypeAbstractFactory[MiddlewareType, Middleware]):
    """Registry for Middleware components"""
    ...


class MiddlewarePipeline:
    """
    This is an implementation of a middleware interceptor model pipeline. It is an
    hybrid of the wrapper and processing-stage models by leveraging the nested call structure
    of the wrapper model and the intercepter concept from  the processing-stage model.
    This can be achieved by adding a data container (RequestExchange) that is passed between
    each middleware element in the pipeline model.
    """

    def __init__(self) -> None:
        self._middleware_list: list[MIDDLEWARE_FUNC] = []

    def add(self, middleware: MIDDLEWARE_FUNC) -> None:
        self._middleware_list.append(middleware)

    async def execute(
        self,
        initial: RequestExchange,
        terminal_handler: NEXT_CALL,
    ) -> RequestExchange:
        """
        Nests the middleware by index in the order defined in _middleware_list.
        """

        async def _run(index: int, req: RequestExchange) -> RequestExchange:
            if index < len(self._middleware_list):
                mw = self._middleware_list[index]

                async def next_step(r: RequestExchange) -> RequestExchange:
                    return await _run(index + 1, r)

                return await mw(req, next_step)
            else:
                return await terminal_handler(req)

        return await _run(0, initial)
