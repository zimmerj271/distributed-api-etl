import pytest
from unittest.mock import patch, MagicMock
from auth.token.token_provider import (
    StaticTokenProvider,
    RpcTokenProvider,
    PasswordGrantTokenProvider,
    FallbackTokenProvider
)
from tests.fixtures.auth_token import (
    FakeTokenProvider,
    FailingTokenProvider,
    valid_token,
)


@pytest.mark.asyncio
async def test_static_token_provider():
    provider = StaticTokenProvider("static-token")
    token = await provider.get_token()

    assert token.token_value == "static-token"
    assert token.expires_at is not None


@pytest.mark.asyncio
async def test_rpc_token_provider_failure(monkeypatch):
    async def fake_sleep(*args, **kwargs):
        return None

    monkeypatch.setattr("auth.token.token_provider.async_exponential_backoff", fake_sleep)

    provider = RpcTokenProvider("http://bad-url", max_retries=2)

    with pytest.raises(RuntimeError):
        await provider.get_token()


@pytest.mark.asyncio
@patch("auth.token.token_provider.requests.post")
async def test_password_grant_success(mock_post):
    response = MagicMock()
    response.json.return_value = {
        "access_token": "token123",
        "expires_in": 60,
    }
    response.raise_for_status.return_value = None
    mock_post.return_value = response

    provider = PasswordGrantTokenProvider(
        token_url="http://token",
        client_id="id",
        client_secret="secret",
        username="user",
        password="pass",
    )

    token = await provider.get_token()

    assert token.token_value == "token123"
    assert token.expires_at is not None


@pytest.mark.asyncio
async def test_fallback_uses_primary():
    primary = FakeTokenProvider(valid_token())
    fallback = FakeTokenProvider(valid_token())

    provider = FallbackTokenProvider(primary, fallback)
    token = await provider.get_token()

    assert token.token_value == "abc123"
    assert primary.calls == 1
    assert fallback.calls == 0


@pytest.mark.asyncio
async def test_fallback_uses_fallback_on_failure():
    primary = FailingTokenProvider()
    fallback = FakeTokenProvider(valid_token())

    provider = FallbackTokenProvider(primary, fallback)
    token = await provider.get_token()

    assert token.token_value == "abc123"

