"""Unit tests for transport send error handling"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from request_execution.transport.engine import AiohttpEngine
from request_execution.transport.base import TransportRequest
from tests.fixtures.request_execution.transport import tcp_config_no_tls


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestSendErrorHandling:
    """Tests for error handling during send"""

    async def test_send_without_session_raises(self):
        """
        GIVEN an AiohttpEngine without session
        WHEN send is called
        THEN it should raise ValueError (session not assigned)
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        req = TransportRequest(
            method="GET",
            url="test",
            headers={},
        )

        with pytest.raises(ValueError, match="ClientSession not assigned"):
            await engine.send(req)

    async def test_send_catches_client_errors(self):
        """
        GIVEN a session that raises ClientError
        WHEN send is called
        THEN it should return error in TransportResponse
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        # Create a mock context manager for the request
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("Connection failed"))
        mock_cm.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_cm)

        engine._session = mock_session

        req = TransportRequest(
            method="GET",
            url="test",
            headers={},
        )

        resp = await engine.send(req)

        assert resp.status is None
        assert resp.error is not None
        assert "RuntimeError" in resp.error
        assert "Connection failed" in resp.error

    async def test_send_error_includes_exception_type(self):
        """
        GIVEN a request that raises an exception
        WHEN send is called
        THEN error should include exception type name
        """
        import aiohttp

        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        # Create a mock context manager for the request
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("Failed")
        )
        mock_cm.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_cm)

        engine._session = mock_session

        req = TransportRequest(
            method="GET",
            url="test",
            headers={},
        )

        resp = await engine.send(req)

        assert "ClientConnectionError" in resp.error

    async def test_send_error_has_empty_headers(self):
        """
        GIVEN a request that fails
        WHEN send is called
        THEN response should have empty headers dict
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        # Create a mock context manager for the request
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("Boom"))
        mock_cm.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_cm)

        engine._session = mock_session

        req = TransportRequest(
            method="GET",
            url="test",
            headers={},
        )

        resp = await engine.send(req)

        assert resp.headers == {}
        assert resp.body is None


@pytest.mark.unit
@pytest.mark.transport
@pytest.mark.asyncio
class TestSendSuccess:
    """Tests for successful send operations"""

    async def test_send_returns_response_data(self):
        """
        GIVEN a successful request
        WHEN send is called
        THEN it should return response data
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.read = AsyncMock(return_value=b'{"ok": true}')

        # Create a mock context manager for the request
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_cm)

        engine._session = mock_session

        req = TransportRequest(
            method="GET",
            url="test",
            headers={},
        )

        resp = await engine.send(req)

        assert resp.status == 200
        assert resp.headers["Content-Type"] == "application/json"
        assert resp.body == b'{"ok": true}'
        assert resp.error is None

    async def test_send_passes_request_parameters(self):
        """
        GIVEN a TransportRequest with all parameters
        WHEN send is called
        THEN all parameters should be passed to aiohttp
        """
        engine = AiohttpEngine(
            base_url="https://example.com",
            connector_config=tcp_config_no_tls(),
        )

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {}
        mock_response.read = AsyncMock(return_value=b"")

        # Create a mock context manager for the request
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_cm)

        engine._session = mock_session

        req = TransportRequest(
            method="POST",
            url="/api/endpoint",
            headers={"X-Custom": "value"},
            params={"q": "search"},
            json={"data": "payload"},
        )

        await engine.send(req)

        # Verify request was called with correct parameters
        call_args = mock_session.request.call_args
        assert call_args[0][0] == "POST"  # method
        assert call_args[0][1] == "/api/endpoint"  # url
        assert call_args.kwargs["headers"]["X-Custom"] == "value"
        assert call_args.kwargs["params"]["q"] == "search"
        assert call_args.kwargs["json"]["data"] == "payload"
