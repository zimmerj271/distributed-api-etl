import ssl
from unittest.mock import patch

from transport.engine import AiohttpEngine
from config.models.transport import TlsConfig
from tests.fixtures.transport import tcp_config_no_tls


@patch("transport.engine.ssl.create_default_context")
def test_build_ssl_context_verify_disabled(mock_create):
    mock_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    mock_create.return_value = mock_ctx

    tls = TlsConfig(enabled=True, verify=False)

    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
        tls_config=tls,
    )

    ctx = engine._build_ssl_context(tls)

    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False

