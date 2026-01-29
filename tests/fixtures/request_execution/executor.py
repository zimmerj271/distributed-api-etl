from request_execution.models import RequestContext, RequestType


def basic_request_context() -> RequestContext:
    return RequestContext(
        method=RequestType.GET,
        url="/test/resource",
        headers={"X-Test": "1"},
        params={"q": "value"},
    )
