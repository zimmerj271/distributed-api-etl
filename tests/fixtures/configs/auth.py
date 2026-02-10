import pytest
from config.models.auth import (
    NoAuthConfig,
    BasicAuthConfig,
    BearerTokenConfig,
    OAuth2Config,
)
from auth.strategy import AuthType


@pytest.fixture
def no_auth():
    return NoAuthConfig(type=AuthType.NONE)


@pytest.fixture
def basic_auth():
    return BasicAuthConfig(
        type=AuthType.BASIC,
        username="user",
        password="pass",
    )


@pytest.fixture
def bearer_auth():
    return BearerTokenConfig(
        type=AuthType.BEARER,
        token="abc123",
    )


@pytest.fixture
def oauth2_password_auth():
    return OAuth2Config(
        type=AuthType.OAUTH2_PASSWORD,
        token_url="https://auth/token",
        client_id="client",
        client_secret="secret",
        username="user",
        password="pass",
        refresh_margin=120,
    )


@pytest.fixture
def oauth2_client_credentials_auth():
    return OAuth2Config(
        type=AuthType.OAUTH2_CLIENT_CREDENTIALS,
        token_url="https://auth/token",
        client_id="client",
        client_secret="secret",
        refresh_margin=120,
    )
