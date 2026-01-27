import logging
from typing import Protocol, Any

from transport.base import TransportEngine
from clients.base import RequestContext, RequestExchange 
from transport.base import TransportRequest
from middleware.pipeline import MIDDLEWARE_FUNC, MiddlewarePipeline


class ApiClient:
    """
    The API Client is a thin orchestration layer that manages the interface between the semantic 
    ETL layer and the Transport Layer. ApiClient then acts as a gateway between the two layers and
    is completely decoupled from network I/O. ApiClient is resopnsible for the folowing:
    • Owns and runs the middleware pipeline.
    • Creates a TransportEngine via TransportFactory to interface with the Transport layer.
    • Create appropriate session configuration
    • Interface between the ETL Layer and the Transport Layer 
    """

    def __init__(
            self, 
            transport: TransportEngine,
            logger: logging.Logger | None = None
    ) -> None: 
        self.transport = transport
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._pipeline = MiddlewarePipeline()

    def add_middleware(self, middleware: MIDDLEWARE_FUNC) -> None:
        self._pipeline.add(middleware)


    async def send(self, context: RequestContext) -> RequestExchange:
        """
        Execute a single HTTP request defined by the RequestContext through
        the interceptor middleware pipeline and underlying Transport layer.
        """

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
                req.success = transport_response.status is not None and transport_response.status < 500

            return req

        initial = RequestExchange(context=context)
        return await self._pipeline.execute(initial, terminal)


class TokenHttpClient(Protocol):
    """Structural interface for a narrow HTTP client interface for authorization flows."""

    async def post_form(
        self,
        url: str,
        data: dict[str, str],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        ...
