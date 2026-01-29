"""Unit tests for AiohttpEngine initialization"""
import pytest
from aiohttp import TCPConnector, ClientTimeout
from request_execution.transport.engine import AiohttpEngine
from tests.fixtures.request_execution.transport import tcp_config_no_tls, tls_config_disabled


@pytest.mark.unit
@pytest.mark.transport
class TestAiohttpEngineInitialization:
    """Tests for AiohttpEngine initialization"""
    
    def test_initializes_with_required_params(self):
        """
        GIVEN required initialization parameters
        WHEN AiohttpEngine is created
        THEN it should initialize successfully
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._base_url == "https://example.com"
        assert engine._connector is not None
    
    def test_strips_trailing_slash_from_base_url(self):
        """
        GIVEN base_url with trailing slash
        WHEN AiohttpEngine is created
        THEN trailing slash should be removed
        """
        engine = AiohttpEngine(
            base_url="https://example.com/",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._base_url == "https://example.com"
    
    def test_preserves_base_url_without_trailing_slash(self):
        """
        GIVEN base_url without trailing slash
        WHEN AiohttpEngine is created
        THEN URL should remain unchanged
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._base_url == "https://example.com"
    
    def test_handles_base_url_with_path(self):
        """
        GIVEN base_url with path and trailing slash
        WHEN AiohttpEngine is created
        THEN only trailing slash should be removed
        """
        engine = AiohttpEngine(
            base_url="https://api.example.com/v2/",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._base_url == "https://api.example.com/v2"


@pytest.mark.unit
@pytest.mark.transport
class TestAiohttpEngineConnectorConfiguration:
    """Tests for TCP connector configuration"""
    
    def test_builds_tcp_connector(self):
        """
        GIVEN connector configuration
        WHEN AiohttpEngine is created
        THEN it should build TCPConnector
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert isinstance(engine._connector, TCPConnector)
    
    def test_connector_respects_limit(self):
        """
        GIVEN connector config with connection limit
        WHEN AiohttpEngine is created
        THEN connector should have correct limit
        """
        from config.models.transport import TcpConnectionConfig
        
        config = TcpConnectionConfig(limit=50)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=config,
        )
        
        assert engine._connector.limit == 50
    
    def test_connector_respects_limit_per_host(self):
        """
        GIVEN connector config with per-host limit
        WHEN AiohttpEngine is created
        THEN connector should have correct per-host limit
        """
        from config.models.transport import TcpConnectionConfig
        
        config = TcpConnectionConfig(limit=100, limit_per_host=10)
        
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=config,
        )
        
        assert engine._connector.limit_per_host == 10


@pytest.mark.unit
@pytest.mark.transport
class TestAiohttpEngineTimeoutConfiguration:
    """Tests for timeout configuration"""
    
    def test_sets_base_timeout(self):
        """
        GIVEN base_timeout parameter
        WHEN AiohttpEngine is created
        THEN timeout should be configured correctly
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            base_timeout=45,
        )
        
        assert isinstance(engine._timeout, ClientTimeout)
        assert engine._timeout.total == 45
    
    def test_default_timeout_is_30_seconds(self):
        """
        GIVEN no timeout parameter
        WHEN AiohttpEngine is created
        THEN default timeout should be 30 seconds
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._timeout.total == 30
    
    def test_sets_warmup_timeout(self):
        """
        GIVEN warmup_timeout parameter
        WHEN AiohttpEngine is created
        THEN warmup timeout should be configured
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            warmup_timeout=5,
        )
        
        assert engine._warmup_timeout.total == 5


@pytest.mark.unit
@pytest.mark.transport
class TestAiohttpEngineInitialState:
    """Tests for initial state after construction"""
    
    def test_session_is_none_initially(self):
        """
        GIVEN newly created AiohttpEngine
        WHEN not yet used as context manager
        THEN session should be None
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._session is None
    
    def test_warmup_state_is_false_initially(self):
        """
        GIVEN newly created AiohttpEngine
        WHEN not yet warmed up
        THEN _warmed_up should be False
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._warmed_up is False
    
    def test_warmup_error_is_none_initially(self):
        """
        GIVEN newly created AiohttpEngine
        WHEN no warmup attempted
        THEN _warmup_error should be None
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert engine._warmup_error is None
