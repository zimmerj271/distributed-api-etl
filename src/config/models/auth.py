from abc import ABC
from typing import Annotated, Union, Any, Literal, TypeVar, Generic
from pydantic import Field, BaseModel

from auth.strategy import AuthType


T = TypeVar("T", bound=AuthType)


class AuthConfigModel(BaseModel, ABC, Generic[T]):
    """Base config for all auth types."""

    type: T

    model_config = {"frozen": True}

    def to_runtime_args(self) -> dict[str, Any]:
        return {}


class NoAuthConfig(AuthConfigModel):
    type: Literal[AuthType.NONE] = AuthType.NONE


class BasicAuthConfig(AuthConfigModel):
    type: Literal[AuthType.BASIC] = AuthType.BASIC
    username: str
    password: str

    def to_runtime_args(self) -> dict[str, Any]:
        return {
            "username": self.username,
            "password": self.password,
        }


class BearerTokenConfig(AuthConfigModel):
    type: Literal[AuthType.BEARER] = AuthType.BEARER
    token: str

    def to_runtime_args(self) -> dict[str, Any]:
        return {
            "token": self.token,
        }


class OAuth2Config(AuthConfigModel):
    type: Literal[AuthType.OAUTH2_PASSWORD, AuthType.OAUTH2_CLIENT_CREDENTIALS] = (
        AuthType.OAUTH2_CLIENT_CREDENTIALS
    )
    token_url: str
    client_id: str
    client_secret: str
    username: str | None = None
    password: str | None = None
    refresh_margin: int = 60

    def to_runtime_args(self) -> dict[str, Any]:
        args = {
            "token_url": self.token_url,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_margin": self.refresh_margin,
        }
        # Only include username/password if set (for password grant)
        if self.username is not None:
            args["username"] = self.username
        if self.password is not None:
            args["password"] = self.password
        return args


AuthConfigUnion = Annotated[
    Union[
        NoAuthConfig,
        BasicAuthConfig,
        BearerTokenConfig,
        OAuth2Config,
    ],
    Field(discriminator="type"),
]
