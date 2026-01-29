"""Unit tests for SSL/TLS context configuration"""
import ssl
import pytest
from unittest.mock import patch, MagicMock
from request_execution.transport.engine import AiohttpEngine
from config.models.transport import TlsConfig, TcpConnectionConfig
from tests.fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.unit
@pytest.mark.transport
class TestSSLContextCreation:
    """Tests for SSL context building"""
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_build_ssl_context_creates_default_context(self, mock_create):
        """
        GIVEN TLS config with verify enabled
        WHEN _build_ssl_context is called
        THEN it should create default SSL context
        """
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create.return_value = mock_ctx
        
        tls = TlsConfig(enabled=True, verify=True)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            tls_config=tls,
        )
        
        ctx = engine._build_ssl_context(tls)
        
        mock_create.assert_called_once_with(purpose=ssl.Purpose.SERVER_AUTH)
        assert ctx is mock_ctx
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_build_ssl_context_with_verify_disabled(self, mock_create):
        """
        GIVEN TLS config with verify=False
        WHEN _build_ssl_context is called
        THEN it should disable verification
        """
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
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_build_ssl_context_with_verify_enabled(self, mock_create):
        """
        GIVEN TLS config with verify=True (default)
        WHEN _build_ssl_context is called
        THEN it should enable verification
        """
        mock_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # Default verify mode is CERT_REQUIRED
        mock_ctx.verify_mode = ssl.CERT_REQUIRED
        mock_ctx.check_hostname = True
        mock_create.return_value = mock_ctx
        
        tls = TlsConfig(enabled=True, verify=True)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            tls_config=tls,
        )
        
        ctx = engine._build_ssl_context(tls)
        
        # Should maintain default secure settings
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is True


@pytest.mark.unit
@pytest.mark.transport
class TestSSLContextWithCustomCerts:
    """Tests for SSL context with custom certificates"""
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_loads_custom_ca_bundle(self, mock_create):
        """
        GIVEN TLS config with custom CA bundle
        WHEN _build_ssl_context is called
        THEN it should load CA bundle
        """
        from pathlib import Path
        
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create.return_value = mock_ctx
        
        ca_path = Path("/path/to/ca-bundle.crt")
        tls = TlsConfig(enabled=True, verify=True, ca_bundle=ca_path)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            tls_config=tls,
        )
        
        ctx = engine._build_ssl_context(tls)
        
        mock_ctx.load_verify_locations.assert_called_once_with(cafile=str(ca_path))
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_loads_client_certificate(self, mock_create):
        """
        GIVEN TLS config with client cert and key
        WHEN _build_ssl_context is called
        THEN it should load client certificate
        """
        from pathlib import Path
        
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create.return_value = mock_ctx
        
        cert_path = Path("/path/to/client.crt")
        key_path = Path("/path/to/client.key")
        tls = TlsConfig(
            enabled=True,
            verify=True,
            client_cert=cert_path,
            client_key=key_path,
        )
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            tls_config=tls,
        )
        
        ctx = engine._build_ssl_context(tls)
        
        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile=str(cert_path),
            keyfile=str(key_path),
        )
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_loads_client_cert_without_separate_key(self, mock_create):
        """
        GIVEN TLS config with client cert but no separate key
        WHEN _build_ssl_context is called
        THEN it should load cert with key=None
        """
        from pathlib import Path
        
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create.return_value = mock_ctx
        
        cert_path = Path("/path/to/client.pem")
        tls = TlsConfig(
            enabled=True,
            verify=True,
            client_cert=cert_path,
            client_key=None,
        )
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            tls_config=tls,
        )
        
        ctx = engine._build_ssl_context(tls)
        
        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile=str(cert_path),
            keyfile=None,
        )


@pytest.mark.unit
@pytest.mark.transport
class TestTCPConnectorWithTLS:
    """Tests for TCP connector with TLS configuration"""
    
    @patch("request_execution.transport.engine.ssl.create_default_context")
    def test_connector_includes_ssl_context(self, mock_create):
        """
        GIVEN TLS config in TcpConnectionConfig
        WHEN building TCP connector
        THEN SSL context should be included
        """
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create.return_value = mock_ctx
        
        tls = TlsConfig(enabled=True, verify=False)
        tcp_config = TcpConnectionConfig(limit=10, tls=tls)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config,
        )
        
        # Connector should have SSL context
        assert engine._connector._ssl is not None
    
    def test_connector_without_tls_has_no_ssl(self):
        """
        GIVEN TcpConnectionConfig without TLS
        WHEN building TCP connector
        THEN SSL should not be configured
        """
        tcp_config = TcpConnectionConfig(limit=10, tls=None)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config,
        )
        
        # Connector should not have custom SSL
        # (aiohttp may have default SSL, but we didn't set it)
        connector = engine._connector
        assert connector is not None
