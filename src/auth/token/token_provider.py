import aiohttp
import random
import requests
import time
import asyncio
import logging
from typing import Any, Mapping
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from requests import RequestException, HTTPError

from utils.common import async_exponential_backoff
from auth.token.models import Token


class TokenProvider(ABC):
    @abstractmethod
    async def get_token(self) -> Token: ...

    @abstractmethod
    def token_telemetry(self) -> Mapping[str, Any]: ...


class OAuth2TokenProvider(TokenProvider):
    """
    Concrete implementation of TokenProvider using synchronous requests.
    This TokenProvider requires two sets of credentials:
    1) client_id and client_secret which are loaded into the header.
    2) username and password which are loaded into the data parameter of the request.
    """

    MAX_ATTEMPTS = 5
    BASE_DELAY = 1.0
    MAX_DELAY = 10.0

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        default_expiration: int = 300,
    ) -> None:
        self._url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._default_expiration = default_expiration
        self._logger = logging.Logger(self.__class__.__name__)

    @abstractmethod
    def build_request_body(self) -> dict[str, str]: ...

    async def get_token(self) -> Token:
        def _sync_call() -> Token:
            data = self.build_request_body()
            auth = (self._client_id, self._client_secret)

            last_exc: Exception | None = None

            for attempt in range(1, self.MAX_ATTEMPTS + 1):
                try:
                    try:
                        response = requests.post(
                            self._url,
                            data=data,
                            auth=auth,
                            timeout=10,
                        )
                        response.raise_for_status()
                    except Exception as e:
                        self._logger.error(f"[TokenProvider] Failed to get token: {e}")
                        raise

                    payload: dict[str, Any] = response.json()
                    access_token = payload["access_token"]
                    expires_in = int(
                        payload.get("expires_in", self._default_expiration)
                    )
                    expires_at = datetime.now(timezone.utc) + timedelta(
                        seconds=expires_in
                    )
                    self._logger.info(
                        "[TokenProvider] Successfully retrieved token from the vendor."
                    )
                    return Token(token_value=access_token, expires_at=expires_at)
                except RuntimeError as re:
                    raise re
                except (RequestException, HTTPError, ValueError) as exc:
                    last_exc = exc

                    if attempt >= self.MAX_ATTEMPTS:
                        break

                    delay = min(self.BASE_DELAY * (2 ** (attempt - 1)), self.MAX_DELAY)

                    delay += random.uniform(0, 0.5)

                    self._logger.warning(
                        "Token request failed "
                        f"(attempt {attempt}/{self.MAX_ATTEMPTS} "
                        f"Retrying in {delay:.2f}s: {exc}"
                    )

                    time.sleep(delay)

            self._logger.error("Exhausted retries retrieving token", exc_info=last_exc)

            raise last_exc or RuntimeError("Failed to retrive token")

        return await asyncio.to_thread(_sync_call)

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": self.__class__.__name__, "path": "token_url"}


class PasswordGrantTokenProvider(OAuth2TokenProvider):
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        default_expiration: int = 300,
    ) -> None:
        super().__init__(token_url, client_id, client_secret, default_expiration)
        self._username = username
        self._password = password

    def build_request_body(self) -> dict[str, str]:
        return {
            "grant_type": "password",
            "username": self._username,
            "password": self._password,
        }


class ClientGrantTokenProvider(OAuth2TokenProvider):
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        default_expiration: int = 300,
    ) -> None:
        super().__init__(token_url, client_id, client_secret, default_expiration)

    def build_request_body(self) -> dict[str, str]:
        return {"grant_type": "client_credentials"}


class RpcTokenProvider(TokenProvider):
    """
    Worker side TokenProvider that extracts a token from the RPC managed
    by the driver.
    """

    def __init__(
        self,
        rpc_url: str,
        timeout: int = 10,
        max_retries: int = 5,
        base_delay: float = 0.25,
    ) -> None:
        self._rpc_url = rpc_url
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._logger = logging.Logger(self.__class__.__name__)

    async def get_token(self) -> Token:
        exc = None
        for attempt in range(1, self._max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{self._rpc_url}/token", timeout=self._timeout
                    ) as response:
                        response.raise_for_status()
                        payload = await response.json()

                expires_at = datetime.fromisoformat(payload["expires_at"])
                return Token(token_value=payload["token_value"], expires_at=expires_at)

            except Exception as e:
                exc = e
                await async_exponential_backoff(self._base_delay, attempt)

        raise RuntimeError(f"RPC token service unreachable: {exc}") from exc

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": self.__class__.__name__, "path": "rpc"}


class StaticTokenProvider(TokenProvider):
    def __init__(self, token: str) -> None:
        self._token = token

    async def get_token(self) -> Token:
        return Token(
            token_value=self._token,
            expires_at=datetime.max.replace(tzinfo=timezone.utc),
        )

    def token_telemetry(self) -> dict[str, Any]:
        return {"provider": self.__class__.__name__, "path": "static"}


class FallbackTokenProvider(TokenProvider):
    """
    A thin wrapper class to manage a primary token provider and a
    secondary fallback token provider if the primary fails.
    """

    def __init__(self, primary: TokenProvider | None, fallback: TokenProvider) -> None:
        self._primary = primary
        self._fallback = fallback
        self._last_provider: str | None = None
        self._last_path: str | None = None
        self._last_provider_telemetry: Mapping[str, Any] | None = None

    async def get_token(self) -> Token:
        try:
            if self._primary is None:
                raise RuntimeError(
                    f"Failed to fetch primary TokenProvider {self._primary.__class__.__name__}"
                )

            token = await self._primary.get_token()

            self._last_provider_telemetry = self._primary.token_telemetry()

            if token is None:
                raise RuntimeError(
                    f"Failed to retrieve token from primary TokenProvider {self._primary.__class__.__name__}"
                )

            return token

        except Exception:
            token = await self._fallback.get_token()
            self._last_provider_telemetry = self._fallback.token_telemetry()

            return token

    def token_telemetry(self) -> Mapping[str, Any]:
        return self._last_provider_telemetry or {}
