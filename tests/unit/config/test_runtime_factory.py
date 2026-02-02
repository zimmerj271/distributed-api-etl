import pytest
from unittest.mock import patch
from config.models.middleware import RetryMiddlewareModel
from config.factories import MiddlewareRuntimeFactory, TransportRuntimeFactory


@pytest.mark.unit
@pytest.mark.config
def test_transport_factory_builds_callable(valid_endpoint_config, aiohttp_config):
    factory = TransportRuntimeFactory.build_factory(
        aiohttp_config,
        valid_endpoint_config,
    )

    assert callable(factory)


@pytest.mark.unit
@pytest.mark.config
@patch("config.factories.AiohttpEngine")
def test_transport_factory_invocation(
    mock_engine,
    valid_endpoint_config,
    aiohttp_config,
):
    factory = TransportRuntimeFactory.build_factory(
        aiohttp_config,
        valid_endpoint_config,
    )

    # Invoke factory for side effect only
    factory()

    mock_engine.assert_called_once()
    kwargs = mock_engine.call_args.kwargs

    assert kwargs["base_url"] == valid_endpoint_config.base_url
    assert kwargs["base_timeout"] == aiohttp_config.base_timeout


@pytest.mark.unit
@pytest.mark.config
@patch("config.factories.MiddlewareFactory.create")
def test_middleware_factory(mock_create):
    retry_config = RetryMiddlewareModel(type="retry", max_attempts=3)
    mock_create.return_value = lambda req, ctx: req

    factory = MiddlewareRuntimeFactory.build_factory(retry_config)

    # Invoke factory for side effect only
    factory()

    mock_create.assert_called_once_with(
        retry_config.type,
        **retry_config.to_runtime_args(),
    )


@pytest.mark.unit
@pytest.mark.config
def test_get_factories_returns_factories():
    middleware_cfgs = [RetryMiddlewareModel(type="retry", max_attempts=3)]

    factories = MiddlewareRuntimeFactory.get_factories(middleware_cfgs)

    assert len(factories) == len(middleware_cfgs)
    assert all(callable(f) for f in factories)
