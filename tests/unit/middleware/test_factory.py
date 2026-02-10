from request_execution import MiddlewareFactory, MiddlewareType


def test_middleware_factory_has_registered_types():
    keys = MiddlewareFactory.list_keys()
    assert MiddlewareType.RETRY in keys
    assert MiddlewareType.LOGGING in keys
    assert MiddlewareType.TIMING in keys


def test_middleware_factory_create_retry():
    mw = MiddlewareFactory.create(MiddlewareType.RETRY)
    assert callable(mw)

