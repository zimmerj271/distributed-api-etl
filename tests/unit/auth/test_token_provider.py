"""Unit tests for token provider implementations"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, AsyncMock
from requests import RequestException, HTTPError

from auth.token.token_provider import (
    Token,
    StaticTokenProvider,
    RpcTokenProvider,
    PasswordGrantTokenProvider,
    FallbackTokenProvider,
)
from tests.fixtures.auth import (
    FakeTokenProvider,
    FailingTokenProvider,
    valid_token,
)


@pytest.mark.unit
@pytest.mark.auth
class TestToken:
    """Tests for Token data class"""
    
    def test_token_creation_with_expiry(self):
        """
        GIVEN token value and expiration datetime
        WHEN Token is created
        THEN it should store both values
        """
        expires = datetime(2025, 1, 1, 12, 0, 0)
        token = Token("test-token", expires_at=expires)
        
        assert token.token_value == "test-token"
        assert token.expires_at == expires
    
    def test_token_creation_without_expiry(self):
        """
        GIVEN only a token value
        WHEN Token is created
        THEN expires_at should be None
        """
        token = Token("test-token", expires_at=None)
        
        assert token.token_value == "test-token"
        assert token.expires_at is None
    
    def test_token_is_not_expired_without_expiry(self):
        """
        GIVEN a token with no expiration
        WHEN is_expired is checked
        THEN it should return False (never expires)
        """
        token = Token("x", None)
        assert token.is_expired is False
    
    def test_token_is_expired_when_past_expiry(self):
        """
        GIVEN a token that expired in the past
        WHEN is_expired is checked
        THEN it should return True
        """
        token = Token(
            "x",
            expires_at=datetime.now() - timedelta(seconds=10),
        )
        assert token.is_expired is True
    
    def test_token_is_not_expired_when_future_expiry(self):
        """
        GIVEN a token that expires in the future
        WHEN is_expired is checked
        THEN it should return False
        """
        token = Token(
            "x",
            expires_at=datetime.now() + timedelta(seconds=300),
        )
        assert token.is_expired is False
    
    def test_token_will_expire_within_margin(self):
        """
        GIVEN a token expiring in 30 seconds
        WHEN checking if it will expire within 60 seconds
        THEN it should return True
        """
        token = Token(
            "x",
            expires_at=datetime.now() + timedelta(seconds=30),
        )
        assert token.will_expire_within(60) is True
    
    def test_token_will_not_expire_outside_margin(self):
        """
        GIVEN a token expiring in 120 seconds
        WHEN checking if it will expire within 60 seconds
        THEN it should return False
        """
        token = Token(
            "x",
            expires_at=datetime.now() + timedelta(seconds=120),
        )
        assert token.will_expire_within(60) is False
    
    def test_token_will_not_expire_without_expiry(self):
        """
        GIVEN a token with no expiration
        WHEN checking will_expire_within
        THEN it should return False
        """
        token = Token("x", expires_at=None)
        assert token.will_expire_within(60) is False
    
    def test_token_seconds_until_expiration(self):
        """
        GIVEN a token expiring in the future
        WHEN seconds_until_expiration is called
        THEN it should return approximate seconds remaining
        """
        future = datetime.now() + timedelta(seconds=100)
        token = Token("x", expires_at=future)
        
        seconds = token.seconds_until_expiration()
        
        # Allow some margin for test execution time
        assert 95 <= seconds <= 105
    
    def test_token_serialization_with_expiry(self):
        """
        GIVEN a token with expiration
        WHEN serialize_token is called
        THEN it should return dict with ISO formatted expiry
        """
        token = Token(
            "abc",
            expires_at=datetime(2025, 1, 1, 12, 0, 0),
        )
        payload = token.serialize_token()
        
        assert payload["token_value"] == "abc"
        assert payload["expires_at"].startswith("2025")
        assert "T" in payload["expires_at"]  # ISO format
    
    def test_token_serialization_without_expiry(self):
        """
        GIVEN a token without expiration
        WHEN serialize_token is called
        THEN expires_at should be empty string
        """
        token = Token("abc", expires_at=None)
        payload = token.serialize_token()
        
        assert payload["token_value"] == "abc"
        assert payload["expires_at"] == ""


@pytest.mark.unit
@pytest.mark.auth
class TestStaticTokenProvider:
    """Tests for static token provider"""
    
    @pytest.mark.asyncio
    async def test_returns_provided_token(self):
        """
        GIVEN a static token provider with a fixed token
        WHEN get_token is called
        THEN it should return a Token with that value
        """
        provider = StaticTokenProvider("static-token")
        token = await provider.get_token()

        assert token.token_value == "static-token"
    
    @pytest.mark.asyncio
    async def test_token_never_expires(self):
        """
        GIVEN a static token provider
        WHEN get_token is called
        THEN the token should have far-future expiration
        """
        provider = StaticTokenProvider("static-token")
        token = await provider.get_token()

        assert token.expires_at is not None
        # Should expire very far in the future (datetime.max)
        assert token.expires_at.year > 9000
    
    @pytest.mark.asyncio
    async def test_always_returns_same_value(self):
        """
        GIVEN a static token provider
        WHEN get_token is called multiple times
        THEN it should always return the same token value
        """
        provider = StaticTokenProvider("constant")
        
        token1 = await provider.get_token()
        token2 = await provider.get_token()
        
        assert token1.token_value == token2.token_value == "constant"
    
    def test_telemetry_identifies_provider(self):
        """
        GIVEN a static token provider
        WHEN token_telemetry is called
        THEN it should identify itself
        """
        provider = StaticTokenProvider("test")
        telemetry = provider.token_telemetry()
        
        assert telemetry["provider"] == "StaticTokenProvider"
        assert telemetry["path"] == "static"


@pytest.mark.unit
@pytest.mark.auth
class TestPasswordGrantTokenProvider:
    """Tests for OAuth2 password grant token provider"""
    
    @pytest.mark.asyncio
    @patch("auth.token.token_provider.requests.post")
    async def test_successful_token_fetch(self, mock_post):
        """
        GIVEN a password grant provider
        WHEN the vendor responds with a valid token
        THEN it should return a Token with correct value and expiry
        """
        # Mock successful OAuth2 response
        response = MagicMock()
        response.json.return_value = {
            "access_token": "oauth-token-123",
            "expires_in": 3600,
        }
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )

        token = await provider.get_token()

        assert token.token_value == "oauth-token-123"
        assert token.expires_at is not None
        
        # Verify correct request was made
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["data"]["grant_type"] == "password"
        assert call_kwargs["data"]["username"] == "user"
        assert call_kwargs["data"]["password"] == "pass"
        assert call_kwargs["auth"] == ("client", "secret")
    
    @pytest.mark.asyncio
    @patch("auth.token.token_provider.requests.post")
    async def test_uses_default_expiration_when_missing(self, mock_post):
        """
        GIVEN a vendor response without expires_in
        WHEN get_token is called
        THEN it should use default expiration (300 seconds)
        """
        response = MagicMock()
        response.json.return_value = {
            "access_token": "token",
            # No expires_in field
        }
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
            default_expiration=300,
        )

        token = await provider.get_token()
        
        # Token should expire in approximately 300 seconds
        seconds_remaining = token.seconds_until_expiration()
        assert 295 <= seconds_remaining <= 305
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    @patch("auth.token.token_provider.requests.post")
    async def test_retries_on_transient_failure(self, mock_post):
        """
        GIVEN a provider configured with retries
        WHEN the first request fails but second succeeds
        THEN it should retry and eventually succeed
        """
        # First call fails, second succeeds
        failure_response = MagicMock()
        failure_response.raise_for_status.side_effect = HTTPError("500 Server Error")
        
        success_response = MagicMock()
        success_response.json.return_value = {
            "access_token": "retry-token",
            "expires_in": 60,
        }
        success_response.raise_for_status.return_value = None
        
        mock_post.side_effect = [failure_response, success_response]

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )

        token = await provider.get_token()
        
        assert token.token_value == "retry-token"
        assert mock_post.call_count == 2
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    @patch("auth.token.token_provider.requests.post")
    async def test_raises_after_max_retries_exhausted(self, mock_post):
        """
        GIVEN a provider with max retries
        WHEN all retry attempts fail
        THEN it should raise the last exception
        """
        # All attempts fail
        response = MagicMock()
        response.raise_for_status.side_effect = HTTPError("503 Service Unavailable")
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )
        # MAX_ATTEMPTS = 5 in the implementation

        with pytest.raises(HTTPError, match="503"):
            await provider.get_token()
        
        # Should have tried MAX_ATTEMPTS times
        assert mock_post.call_count == 5
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    @patch("auth.token.token_provider.requests.post")
    async def test_handles_connection_errors(self, mock_post):
        """
        GIVEN a provider
        WHEN a network connection error occurs
        THEN it should retry and eventually raise
        """
        mock_post.side_effect = RequestException("Connection refused")

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )

        with pytest.raises(RequestException, match="Connection refused"):
            await provider.get_token()
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    @patch("auth.token.token_provider.requests.post")
    async def test_handles_invalid_json_response(self, mock_post):
        """
        GIVEN a provider
        WHEN the response has invalid JSON
        THEN it should raise ValueError
        """
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.side_effect = ValueError("Invalid JSON")
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )

        with pytest.raises(ValueError, match="Invalid JSON"):
            await provider.get_token()
    
    def test_telemetry_identifies_provider(self):
        """
        GIVEN a password grant provider
        WHEN token_telemetry is called
        THEN it should identify itself and token URL
        """
        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )
        
        telemetry = provider.token_telemetry()
        
        assert telemetry["provider"] == "PasswordGrantTokenProvider"
        assert telemetry["path"] == "token_url"


@pytest.mark.unit
@pytest.mark.auth
class TestRpcTokenProvider:
    """Tests for RPC token provider (worker-side)"""
    
    @pytest.mark.asyncio
    async def test_fetches_token_from_rpc_service(self):
        """
        GIVEN an RPC token provider pointing to a mock RPC service
        WHEN get_token is called
        THEN it should fetch and deserialize the token
        """
        with patch("auth.token.token_provider.aiohttp.ClientSession") as mock_session_cls:
            # Create mock response
            mock_response = MagicMock()
            mock_response.raise_for_status = AsyncMock()
            mock_response.json = AsyncMock(return_value={
                "token_value": "rpc-token",
                "expires_at": "2025-06-01T12:00:00"
            })
            
            # Create async context manager for response
            mock_response_cm = MagicMock()
            mock_response_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_response_cm.__aexit__ = AsyncMock(return_value=False)
            
            # Create mock session
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=mock_response_cm)
            
            # Create async context manager for session
            mock_session_cm = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_cls.return_value = mock_session_cm

            provider = RpcTokenProvider(
                rpc_url="http://driver:9999",
                timeout=10,
            )

            token = await provider.get_token()

            assert token.token_value == "rpc-token"
            assert isinstance(token.expires_at, datetime)
            
            # Verify correct URL was called
            mock_session.get.assert_called_once()
            call_args, call_kwargs = mock_session.get.call_args
            assert "http://driver:9999/token" in call_args
    
    @pytest.mark.asyncio
    async def test_retries_on_network_failure(self):
        """
        GIVEN an RPC provider with retry configuration
        WHEN the first request fails but second succeeds
        THEN it should retry and eventually return token
        """
        with patch("auth.token.token_provider.aiohttp.ClientSession") as mock_session_cls:
            # First response - failure
            failure_response = MagicMock()
            failure_response.raise_for_status = AsyncMock(
                side_effect=Exception("Connection error")
            )
            
            failure_response_cm = MagicMock()
            failure_response_cm.__aenter__ = AsyncMock(return_value=failure_response)
            failure_response_cm.__aexit__ = AsyncMock(return_value=False)
            
            # Second response - success
            success_response = MagicMock()
            success_response.raise_for_status = AsyncMock()
            success_response.json = AsyncMock(return_value={
                "token_value": "retry-success",
                "expires_at": "2025-06-01T12:00:00"
            })
            
            success_response_cm = MagicMock()
            success_response_cm.__aenter__ = AsyncMock(return_value=success_response)
            success_response_cm.__aexit__ = AsyncMock(return_value=False)
            
            # Mock session
            mock_session = MagicMock()
            mock_session.get = MagicMock(
                side_effect=[failure_response_cm, success_response_cm]
            )
            
            mock_session_cm = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_cls.return_value = mock_session_cm

            # Mock the backoff delay to speed up test
            with patch("auth.token.token_provider.async_exponential_backoff", new_callable=AsyncMock):
                provider = RpcTokenProvider(
                    rpc_url="http://driver:9999",
                    timeout=10,
                    max_retries=5,
                )

                token = await provider.get_token()
                
                assert token.token_value == "retry-success"
                assert mock_session.get.call_count == 2
     
    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        """
        GIVEN an RPC provider with max_retries=3
        WHEN all 3 attempts fail
        THEN it should raise RuntimeError
        """
        with patch("auth.token.token_provider.aiohttp.ClientSession") as mock_session_cls:
            # All responses fail
            failure_response = MagicMock()
            failure_response.raise_for_status = AsyncMock(
                side_effect=Exception("Network error")
            )
            
            failure_response_cm = MagicMock()
            failure_response_cm.__aenter__ = AsyncMock(return_value=failure_response)
            failure_response_cm.__aexit__ = AsyncMock(return_value=False)
            
            mock_session = MagicMock()
            mock_session.get = MagicMock(return_value=failure_response_cm)
            
            mock_session_cm = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_cls.return_value = mock_session_cm

            # Mock backoff to speed up test
            with patch("auth.token.token_provider.async_exponential_backoff", new_callable=AsyncMock):
                provider = RpcTokenProvider(
                    rpc_url="http://driver:9999",
                    max_retries=3,
                )

                with pytest.raises(RuntimeError, match="RPC token service unreachable"):
                    await provider.get_token()
                
                # Should have tried max_retries times
                assert mock_session.get.call_count == 3

    @pytest.mark.asyncio
    async def test_respects_timeout(self):
        """
        GIVEN an RPC provider with a timeout
        WHEN making requests
        THEN it should configure the session with that timeout
        """
        with patch("auth.token.token_provider.aiohttp.ClientSession") as mock_session_cls:
            with patch("auth.token.token_provider.aiohttp.ClientTimeout") as mock_timeout_cls:
                # Mock successful response
                mock_response = MagicMock()
                mock_response.raise_for_status = AsyncMock()
                mock_response.json = AsyncMock(return_value={
                    "token_value": "test",
                    "expires_at": "2025-06-01T12:00:00"
                })
                
                mock_response_cm = MagicMock()
                mock_response_cm.__aenter__ = AsyncMock(return_value=mock_response)
                mock_response_cm.__aexit__ = AsyncMock(return_value=False)
                
                mock_session = MagicMock()
                mock_session.get = MagicMock(return_value=mock_response_cm)
                
                mock_session_cm = MagicMock()
                mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_cm.__aexit__ = AsyncMock(return_value=False)
                
                mock_session_cls.return_value = mock_session_cm

                provider = RpcTokenProvider(
                    rpc_url="http://driver:9999",
                    timeout=15,
                )

                await provider.get_token()

                # Verify timeout was configured
                mock_timeout_cls.assert_called_with(total=15)
    
    def test_telemetry_identifies_provider(self):
        """
        GIVEN an RPC token provider
        WHEN token_telemetry is called
        THEN it should identify itself as RPC provider
        """
        provider = RpcTokenProvider(
            rpc_url="http://driver:9999",
            timeout=10,
        )
        
        telemetry = provider.token_telemetry()
        
        assert telemetry["provider"] == "RpcTokenProvider"
        assert telemetry["path"] == "rpc"


@pytest.mark.unit
@pytest.mark.auth
class TestFallbackTokenProvider:
    """Tests for fallback token provider logic"""
    
    @pytest.mark.asyncio
    async def test_uses_primary_when_successful(self):
        """
        GIVEN a fallback provider with working primary
        WHEN get_token is called
        THEN it should use primary and not touch fallback
        """
        primary = FakeTokenProvider(valid_token())
        fallback = FakeTokenProvider(valid_token())

        provider = FallbackTokenProvider(primary, fallback)
        token = await provider.get_token()

        assert token.token_value == "abc123"
        assert primary.calls == 1
        assert fallback.calls == 0
    
    @pytest.mark.asyncio
    async def test_falls_back_when_primary_fails(self):
        """
        GIVEN a fallback provider with failing primary
        WHEN get_token is called
        THEN it should fall back to secondary provider
        """
        primary = FailingTokenProvider()
        fallback = FakeTokenProvider(valid_token())

        provider = FallbackTokenProvider(primary, fallback)
        token = await provider.get_token()

        assert token.token_value == "abc123"
        assert fallback.calls == 1
    
    @pytest.mark.asyncio
    async def test_handles_none_primary_provider(self):
        """
        GIVEN a fallback provider with None as primary
        WHEN get_token is called
        THEN it should immediately use fallback
        """
        fallback = FakeTokenProvider(valid_token())
        provider = FallbackTokenProvider(None, fallback)
        
        token = await provider.get_token()
        
        assert token.token_value == "abc123"
        assert fallback.calls == 1
    
    @pytest.mark.asyncio
    async def test_propagates_error_if_both_fail(self):
        """
        GIVEN a fallback provider where both providers fail
        WHEN get_token is called
        THEN it should raise the fallback provider's error
        """
        primary = FailingTokenProvider()
        fallback = FailingTokenProvider()

        provider = FallbackTokenProvider(primary, fallback)

        with pytest.raises(RuntimeError, match="provider failed"):
            await provider.get_token()
    
    @pytest.mark.asyncio
    async def test_telemetry_reflects_primary_on_success(self):
        """
        GIVEN a fallback provider that uses primary
        WHEN token_telemetry is called after successful fetch
        THEN it should return primary's telemetry
        """
        primary = FakeTokenProvider(valid_token())
        fallback = FakeTokenProvider(valid_token())
        
        provider = FallbackTokenProvider(primary, fallback)
        await provider.get_token()
        
        telemetry = provider.token_telemetry()
        
        assert telemetry["provider"] == "fake"
    
    @pytest.mark.asyncio
    async def test_telemetry_reflects_fallback_on_primary_failure(self):
        """
        GIVEN a fallback provider that uses fallback
        WHEN token_telemetry is called after primary failure
        THEN it should return fallback's telemetry
        """
        primary = FailingTokenProvider()
        fallback = FakeTokenProvider(valid_token())
        
        provider = FallbackTokenProvider(primary, fallback)
        await provider.get_token()
        
        telemetry = provider.token_telemetry()
        
        assert telemetry["provider"] == "fake"
    
    @pytest.mark.asyncio
    async def test_telemetry_before_any_fetch_returns_empty(self):
        """
        GIVEN a fallback provider
        WHEN token_telemetry is called before any fetch
        THEN it should return empty dict
        """
        primary = FakeTokenProvider(valid_token())
        fallback = FakeTokenProvider(valid_token())
        
        provider = FallbackTokenProvider(primary, fallback)
        telemetry = provider.token_telemetry()
        
        assert telemetry == {}


@pytest.mark.unit
@pytest.mark.auth
class TestPasswordGrantRetryMechanism:
    """Detailed tests for retry logic in PasswordGrantTokenProvider"""
    
    @pytest.mark.asyncio
    @patch("auth.token.token_provider.requests.post")
    @patch("auth.token.token_provider.time.sleep")  # Mock sleep to speed up test
    async def test_exponential_backoff_timing(self, mock_sleep, mock_post):
        """
        GIVEN a provider with exponential backoff
        WHEN retries occur
        THEN delays should follow exponential pattern with jitter
        """
        # All attempts fail
        response = MagicMock()
        response.raise_for_status.side_effect = HTTPError("500")
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )
        # BASE_DELAY = 1.0, MAX_DELAY = 10.0 in implementation

        with pytest.raises(HTTPError):
            await provider.get_token()
        
        # Verify sleep was called with increasing delays
        assert mock_sleep.call_count == 4  # 4 retries after first attempt
        
        # Check that delays increase (with some jitter tolerance)
        delays = [call[0][0] for call in mock_sleep.call_args_list]
        
        # First delay should be around 1.0 + jitter
        assert 1.0 <= delays[0] <= 1.5
        
        # Second delay should be around 2.0 + jitter
        assert 2.0 <= delays[1] <= 2.5
        
        # Third delay should be around 4.0 + jitter
        assert 4.0 <= delays[2] <= 4.5
    
    @pytest.mark.asyncio
    @patch("auth.token.token_provider.requests.post")
    @patch("auth.token.token_provider.time.sleep")
    async def test_respects_max_delay_cap(self, mock_sleep, mock_post):
        """
        GIVEN exponential backoff with MAX_DELAY = 10.0
        WHEN many retries occur
        THEN delay should not exceed MAX_DELAY
        """
        response = MagicMock()
        response.raise_for_status.side_effect = HTTPError("500")
        mock_post.return_value = response

        provider = PasswordGrantTokenProvider(
            token_url="http://auth/token",
            client_id="client",
            client_secret="secret",
            username="user",
            password="pass",
        )

        with pytest.raises(HTTPError):
            await provider.get_token()
        
        delays = [call[0][0] for call in mock_sleep.call_args_list]
        
        # All delays should be <= MAX_DELAY (10.0) + jitter (0.5)
        assert all(delay <= 10.5 for delay in delays)
