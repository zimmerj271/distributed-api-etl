from request_execution.models import RequestExchange, RequestContext, RequestType


def test_request_exchange_build_row():
    ctx = RequestContext(
        method=RequestType.GET,
        url="url",
        headers={},
        params={},
    )

    ex = RequestExchange(
        context=ctx,
        status_code=200,
        body=b"data",
        success=True,
        attempts=2,
    )

    row = ex.build_row("abc123")

    assert row.request_id == "abc123"
    assert row.success is True
    assert row.attempts == 2
    assert row.status_code == 200
