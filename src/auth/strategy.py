from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Protocol, Any 

from pyspark.sql import SparkSession
from auth.token.token_manager import TokenManager
from auth.rpc.bootstrap import RpcBootstrapper
from transport.base import TransportEngine
from auth.token.token_provider import (
    RpcTokenProvider, 
    StaticTokenProvider, 
    PasswordGrantTokenProvider,
    FallbackTokenProvider
)
from middleware.pipeline import MIDDLEWARE_FUNC
from middleware.common import BearerTokenMiddleware, HeaderAuthMiddleware
from core.abstract_factory import TypeAbstractFactory


class AuthType(str, Enum):
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    OAUTH2_PASSWORD = "oauth2_password"


class AuthStrategy(ABC):
    """
    Declarative authentication strategy.
    """

    @abstractmethod
    def get_middleware_factories(self) -> list[Callable[[], MIDDLEWARE_FUNC]]:
        return []


class AuthRuntime(AuthStrategy, ABC):

    @abstractmethod
    def runtime_start(self, *args, **kwargs) -> Any: 
        """Start background services"""
        ...

    @abstractmethod
    def runtime_stop(self) -> None: 
        """Stop background services"""
        ...

class AuthTransport(Protocol):

    def get_transport_factories(self) -> list[Callable[[], TransportEngine]]: ...


class AuthStrategyFactory(TypeAbstractFactory[AuthType, AuthStrategy]):
    pass


@AuthStrategyFactory.register(AuthType.NONE)
class NoAuthStrategy(AuthStrategy):
    """No authentication required"""

    def get_middleware_factories(self) -> list[Callable[[], MIDDLEWARE_FUNC]]:
        return []


@AuthStrategyFactory.register(AuthType.BASIC)
class BasicAuthStrategy(AuthStrategy):
    """HTTP Basic Authentication"""

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def get_middleware_factories(self) -> list[Callable[[], MIDDLEWARE_FUNC]]:
        username = self._username
        password = self._password

        def factory() -> MIDDLEWARE_FUNC:
            return HeaderAuthMiddleware(username, password)

        return [factory]


@AuthStrategyFactory.register(AuthType.BEARER)
class BearerTokenStrategy(AuthStrategy):

    def __init__(self, token: str):
        self._token = token

    def get_middleware_factories(self) -> list[Callable[[], MIDDLEWARE_FUNC]]:

        def factory() -> MIDDLEWARE_FUNC:
            token_provider = StaticTokenProvider(self._token)
            token_manager = TokenManager(token_provider)

            return BearerTokenMiddleware(token_manager)

        return [factory]


@AuthStrategyFactory.register(AuthType.OAUTH2_PASSWORD)
class PasswordGrantStrategy(AuthRuntime):
    """
    OAuth2 Password Grant authentication with RPC token distribution.
    """

    def __init__(
        self,
        token_url: str,
        username: str,
        password: str,
        client_id: str,
        client_secret: str,
        refresh_margin: int = 60,
    ) -> None:
        self._token_url = token_url
        self._username = username
        self._password = password
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_margin = refresh_margin

        # Driver-side resources (not serialized)
        self._bootstrapper: RpcBootstrapper | None = None
        self._rpc_url: str | None = None

    def get_middleware_factories(self) -> list[Callable[[], MIDDLEWARE_FUNC]]:
        """
        Return a factory that creates middleware using RpcTokenProvider.
        The rpc_url will be injected via broadcast context.
        """

        # Ensure values are serialized to prevent SparkContext from leaking onto worker node
        rpc_url = str(self._rpc_url) 
        token_url = str(self._token_url)
        client_id = str(self._client_id)
        client_secret = str(self._client_secret)
        username = str(self._username)
        password = str(self._password)

        def factory() -> MIDDLEWARE_FUNC:

            if rpc_url:
                primary_provider = RpcTokenProvider(
                    rpc_url=rpc_url,
                    timeout=10,
                    max_retries=5,
                    base_delay=0.25
                )
            else:
                primary_provider = None

            fallback_provider = PasswordGrantTokenProvider(
                token_url=token_url,
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password
            )

            token_provider = FallbackTokenProvider(primary_provider, fallback_provider)
            token_manager = TokenManager(provider=token_provider)  # Use standard token manager on worker nodes

            return BearerTokenMiddleware(token_manager)

        return [factory]


    def runtime_start(self, spark: SparkSession) -> dict[str, Any]:
        """Start RPC service and background token refresh on the driver"""

        credentials = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "username": self._username,
            "password": self._password,
        }

        self._bootstrapper = RpcBootstrapper(
            spark=spark,
            token_url=self._token_url,
            credentials=credentials,
            refresh_margin=self._refresh_margin
        )

        self._bootstrapper.start()
        self._rpc_url = self._bootstrapper.url

        return {"rpc_url": self._rpc_url}

    def runtime_stop(self) -> None:
        if self._bootstrapper:
            return self._bootstrapper.stop()

