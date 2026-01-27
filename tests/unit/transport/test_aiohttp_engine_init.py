from transport.engine import AiohttpEngine
from tests.fixtures.transport import tcp_config_no_tls


def test_aiohttp_engine_strips_trailing_slash():
    engine = AiohttpEngine(
        base_url="https://example.com/",
        connector_config=tcp_config_no_tls(),
    )

    assert engine._base_url == "https://example.com"

