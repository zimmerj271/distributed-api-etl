"""Unit tests for AiohttpEngine async context manager"""
import pytest
from aiohttp import ClientSession
from request_execution.transport.engine import AiohttpEngine
from tests.fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineContextManager:
    """Tests for async context manager protocol"""
    
    async def test_aenter_creates_session(self):
        """
        GIVEN an AiohttpEngine
        WHEN entering async context
        THEN it should create a ClientSession
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        async with engine:
            assert engine.session is not None
            assert isinstance(engine.session, ClientSession)
    
    async def test_aenter_returns_self(self):
        """
        GIVEN an AiohttpEngine
        WHEN entering async context
        THEN it should return self
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        async with engine as e:
            assert e is engine
    
    async def test_session_is_open_inside_context(self):
        """
        GIVEN an AiohttpEngine in context
        WHEN checking session state
        THEN session should be open (not closed)
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        async with engine:
            assert engine.session is not None
            assert not engine.session.closed
    
    async def test_aexit_closes_session(self):
        """
        GIVEN an AiohttpEngine that entered context
        WHEN exiting context
        THEN session should be closed
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        async with engine:
            pass

        # After exit, session should be None
        assert engine._session is None
    
    async def test_session_property_raises_when_not_set(self):
        """
        GIVEN an AiohttpEngine without session
        WHEN accessing session property
        THEN it should raise ValueError
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        with pytest.raises(ValueError, match="ClientSession not assigned"):
            _ = engine.session
    
    async def test_multiple_context_entries(self):
        """
        GIVEN an AiohttpEngine
        WHEN entering context multiple times sequentially
        THEN it should work correctly each time
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        async with engine:
            session1 = engine.session

        async with engine:
            session2 = engine.session
        
        # Different sessions for each context
        assert session1 is not session2


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestAiohttpEngineContextManagerExceptions:
    """Tests for exception handling in context manager"""
    
    async def test_aexit_closes_session_on_exception(self):
        """
        GIVEN an AiohttpEngine in context
        WHEN an exception occurs inside context
        THEN session should still be closed
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        with pytest.raises(RuntimeError):
            async with engine:
                raise RuntimeError("Test error")
        
        # Session should be closed despite exception
        assert engine._session is None
    
    async def test_exception_propagates_after_cleanup(self):
        """
        GIVEN an AiohttpEngine in context
        WHEN an exception occurs
        THEN exception should propagate after cleanup
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        with pytest.raises(ValueError, match="User error"):
            async with engine:
                raise ValueError("User error")
