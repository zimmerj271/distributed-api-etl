from typing import Protocol, Any, Callable

from request_execution.middleware.listeners import TransportDiagnosticMiddleware
from request_execution.transport.base import TransportEngine
from request_execution.models import RequestContext, RequestExchange
from request_execution.transport.base import TransportRequest
from request_execution.middleware.pipeline import MIDDLEWARE_FUNC, MiddlewarePipeline


class RequestExecutor:
    """
    The API Client is a thin orchestration layer that manages the interface between the semantic
    ETL layer and the Transport Layer. RequestExecutor then acts as a gateway between the two layers and
    is completely decoupled from network I/O. RequestExecutor is resopnsible for the folowing:
    • Owns and runs the middleware pipeline.
    • Creates a TransportEngine via TransportFactory to interface with the Transport layer.
    • Create appropriate session configuration
    • Interface between the ETL Layer and the Transport Layer
    """

    def __init__(
        self,
        transport: TransportEngine,
        middleware_factories: list[Callable[[], MIDDLEWARE_FUNC]],
        enable_transport_diagnostics: bool = True,
    ) -> None:
        self.transport = transport
        self._middleware_factories = middleware_factories
        self._enable_transport_diagnostics = enable_transport_diagnostics

    async def send(self, context: RequestContext) -> RequestExchange:
        """
        Execute a single HTTP request defined by the RequestContext through
        the interceptor middleware pipeline and underlying Transport layer.
        """

        pipeline = MiddlewarePipeline()

        if self._enable_transport_diagnostics:
            pipeline.add(TransportDiagnosticMiddleware(self.transport))

        for factory in self._middleware_factories:
            pipeline.add(factory())

        async def terminal(req: RequestExchange) -> RequestExchange:
            url = req.context.url.lstrip("/")
            transport_request = TransportRequest(
                method=req.context.method.name,
                url=url,
                headers=req.context.headers,
                params=req.context.params,
                json=req.context.json,
                data=req.context.data,
            )
            transport_response = await self.transport.send(transport_request)

            req.status_code = transport_response.status
            req.headers = dict(transport_response.headers or {})
            req.body = transport_response.body

            if transport_response.error:
                req.success = False
                req.error_message = transport_response.error
            else:
                req.success = (
                    transport_response.status is not None
                    and transport_response.status < 500
                )

            return req

        initial = RequestExchange(context=context)
        return await pipeline.execute(initial, terminal)


class TokenHttpClient(Protocol):
    """Structural interface for a narrow HTTP client interface for authorization flows."""

    async def post_form(
        self,
        url: str,
        data: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]: ...
