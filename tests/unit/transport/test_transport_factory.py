"""Unit tests for TransportEngineFactory"""
import pytest

from request_execution.transport.engine import TransportEngineFactory, AiohttpEngine
from request_execution.transport.base import TransportEngineType

from fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.unit
@pytest.mark.transport
class TestTransportEngineFactoryRegistration:
    """Tests for transport engine factory registration"""
    
    def test_aiohttp_engine_is_registered(self):
        """
        GIVEN TransportEngineFactory
        WHEN list_keys is called
        THEN AIOHTTP should be registered
        """
        keys = TransportEngineFactory.list_keys()
        
        assert TransportEngineType.AIOHTTP in keys
    
    def test_httpx_engine_is_registered(self):
        """
        GIVEN TransportEngineFactory
        WHEN list_keys is called
        THEN HTTPX should be registered
        """
        keys = TransportEngineFactory.list_keys()
        
        assert TransportEngineType.HTTPX in keys


@pytest.mark.unit
@pytest.mark.transport
class TestTransportEngineFactoryCreation:
    """Tests for transport engine creation"""
    
    def test_create_aiohttp_engine(self):
        """
        GIVEN TransportEngineFactory
        WHEN create is called with AIOHTTP type
        THEN it should return AiohttpEngine instance
        """
        engine = TransportEngineFactory.create(
            TransportEngineType.AIOHTTP,
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        assert isinstance(engine, AiohttpEngine)
    
    def test_create_passes_constructor_args(self):
        """
        GIVEN TransportEngineFactory with constructor args
        WHEN create is called
        THEN args should be passed to constructor
        """
        config = tcp_config_no_tls()
        
        engine = TransportEngineFactory.create(
            TransportEngineType.AIOHTTP,
            base_url="https://api.example.com",
            connector_config=config,
            base_timeout=45,
        )
        
        assert engine._base_url == "https://api.example.com"
        assert engine._timeout.total == 45
