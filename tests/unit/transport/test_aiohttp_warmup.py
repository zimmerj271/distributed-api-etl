"""Unit tests for connection warmup behavior"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from request_execution.transport.engine import AiohttpEngine
from tests.fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestWarmupBehavior:
    """Tests for connection warmup functionality"""
    
    async def test_warmup_without_session_sets_error(self):
        """
        GIVEN an AiohttpEngine without session
        WHEN warmup is called
        THEN it should set warmup error
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        await engine.warmup()

        assert engine._warmed_up is False
        assert engine._warmup_error is not None
        assert "session not initialized" in engine._warmup_error.lower()
    
    async def test_warmup_with_session_makes_request(self):
        """
        GIVEN an AiohttpEngine with session
        WHEN warmup is called
        THEN it should make a GET request to base_url
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        # Mock session
        mock_response = AsyncMock()
        mock_response.release = AsyncMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()
        
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        
        engine.session = mock_session
        
        await engine.warmup()
        
        mock_session.get.assert_called_once()
        # Should call get with base_url
        call_args = mock_session.get.call_args
        assert engine._base_url in str(call_args)
    
    async def test_warmup_releases_response(self):
        """
        GIVEN warmup making a request
        WHEN response is received
        THEN it should release the response body
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.release = AsyncMock()
        
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock()
        
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        
        engine.session = mock_session
        
        await engine.warmup()
        
        # Should release response body for connection reuse
        mock_response.release.assert_called_once()
    
    async def test_warmup_sets_warmed_up_flag_on_success(self):
        """
        GIVEN successful warmup
        WHEN warmup completes
        THEN _warmed_up should be True
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        # Mock successful warmup
        mock_response = AsyncMock()
        mock_response.release = AsyncMock()
        
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock()
        
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        
        engine.session = mock_session
        
        await engine.warmup()
        
        assert engine._warmed_up is True
        assert engine._warmup_error is None


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestWarmupErrorHandling:
    """Tests for warmup error scenarios"""
    
    async def test_warmup_exception_sets_error_and_flag(self):
        """
        GIVEN warmup that encounters an exception
        WHEN warmup is called
        THEN it should capture error and set flags
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )
        
        # Mock session that raises exception
        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=RuntimeError("Connection failed"))
        
        engine.session = mock_session
        
        await engine.warmup()
        
        assert engine._warmed_up is False
        assert engine._warmup_error is not None
        assert "Connection failed" in engine._warmup_error
    
    async def test_warmup_uses_warmup_timeout(self):
        """
        GIVEN AiohttpEngine with warmup_timeout
        WHEN warmup is called
        THEN it should use warmup timeout, not base timeout
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
            base_timeout=60,
            warmup_timeout=5,
        )
        
        mock_response = AsyncMock()
        mock_response.release = AsyncMock()
        
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock()
        
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        
        engine.session = mock_session
        
        await engine.warmup()
        
        # Check that get was called with warmup timeout
        call_kwargs = mock_session.get.call_args.kwargs
        assert "timeout" in call_kwargs
        assert call_kwargs["timeout"].total == 5
