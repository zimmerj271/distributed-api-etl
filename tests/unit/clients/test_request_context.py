from request_execution import RequestContext, RequestType


def test_request_context_with_headers():
    ctx = RequestContext(
        method=RequestType.GET,
        url="url",
        headers={"A": "1"},
    )

    ctx = ctx.with_headers(ctx, {"B": "2"})

    assert ctx.headers == {"A": "1", "B": "2"}

