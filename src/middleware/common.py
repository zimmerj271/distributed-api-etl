import base64

from auth.token.token_manager import TokenManager 
from clients.base import RequestExchange 
from middleware.pipeline import Middleware, NEXT_CALL, MiddlewareFactory, MiddlewareType


# Standard middleware - these middleware objects mutate requests

@MiddlewareFactory.register(MiddlewareType.BEARER)
class BearerTokenMiddleware(Middleware):
    """
    Inject a bearer token into the Authorization header using an async token provider.
    """

    def __init__(self, token_manager: TokenManager) -> None:
        self.token_manager = token_manager

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        token_value = await self.token_manager.get_token_value()

        request_exchange.context = request_exchange.context.with_headers(
            request_exchange.context, 
            new_headers={"Authorization": f"Bearer {token_value}"}
        ) 

        request_exchange.metadata["token_provider"] = self.token_manager.provider.token_telemetry()

        return await next_call(request_exchange)


@MiddlewareFactory.register(MiddlewareType.HEADER)
class HeaderAuthMiddleware(Middleware):

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    async def __call__(self, request_exchange: RequestExchange, next_call: NEXT_CALL) -> RequestExchange:
        raw_credentials = f"{self.username}:{self.password}"
        b64_credentials = base64.b64encode(raw_credentials.encode("utf-8")).decode("utf-8")
        auth_header = {"Authorization": f"Basic {b64_credentials}"}

        request_exchange.context = (
            request_exchange.context
            .with_headers(request_exchange.context, auth_header)
        )
        return await next_call(request_exchange)
