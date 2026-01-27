from clients.base import RequestContext, RequestExchange, RequestType


def base_request_context() -> RequestContext:
    return RequestContext(
        method=RequestType.GET,
        url="https://example.com",
        headers={},
        metadata={},
    )


def base_exchange() -> RequestExchange:
    return RequestExchange(context=base_request_context())


async def terminal_handler_ok(req: RequestExchange) -> RequestExchange:
    req.status_code = 200
    return req


async def terminal_handler_fail(req: RequestExchange) -> RequestExchange:
    req.status_code = 500
    return req

