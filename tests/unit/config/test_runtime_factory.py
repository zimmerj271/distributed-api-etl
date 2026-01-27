from unittest.mock import patch
from config.factories import TransportRuntimeFactory, MiddlewareRuntimeFactory
from fixtures.configs.endpoint import valid_endpoint_config
from fixtures.configs.transport import aiohttp_config
from fixtures.middleware import retry_middleware_cfg


def test_transport_factory_builds_callable():
    endpoint_config = valid_endpoint_config()
    transport_config = aiohttp_config()
    factory = TransportRuntimeFactory.build_factory(
        transport_config,
        endpoint_config,
    )

    assert callable(factory)


@patch("config.factories.AiohttpEngine")
def test_transport_factory_invocation(mock_engine):

    endpoint_config = valid_endpoint_config()
    transport_config = aiohttp_config()
    factory = TransportRuntimeFactory.build_factory(
        transport_config,
        endpoint_config,
    )

    # Invoke factory for side effect only
    factory()

    mock_engine.assert_called_once()
    kwargs = mock_engine.call_args.kwargs

    assert kwargs["base_url"] == endpoint_config.base_url
    assert kwargs["base_timeout"] == transport_config.base_timeout


@patch("config.factories.MiddlewareFactory.create")
def test_middleware_factory(mock_create):
    retry_config = retry_middleware_cfg()
    mock_create.return_value = lambda req, ctx: req

    factory = MiddlewareRuntimeFactory.build_factory(retry_config)

    # Invoke factory for side effect only
    factory()

    mock_create.assert_called_once_with(
        retry_middleware_cfg.type,
        **retry_middleware_cfg.to_runtime_args(),
    )


def test_get_factories_returns_factories(middleware_cfgs):

    factories = MiddlewareRuntimeFactory.get_factories(middleware_cfgs)

    assert len(factories) == len(middleware_cfgs)
    assert all(callable(f) for f in factories)


