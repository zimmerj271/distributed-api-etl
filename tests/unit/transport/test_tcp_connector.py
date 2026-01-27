from aiohttp import TCPConnector
from transport.engine import AiohttpEngine
from tests.fixtures.transport import tcp_config_no_tls


def test_build_tcp_connector_without_tls():
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    connector = engine._connector
    assert isinstance(connector, TCPConnector)

