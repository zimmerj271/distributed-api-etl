import ssl
from types import TracebackType
from typing_extensions import Self
from aiohttp import ClientSession, ClientTimeout, TCPConnector

from config.models.transport import TcpConnectionConfig, TlsConfig
from request_execution.models import TransportRequest, TransportResponse
from request_execution.transport.base import TransportEngineType, TransportEngine
from core.abstract_factory import TypeAbstractFactory


class TransportEngineFactory(TypeAbstractFactory[TransportEngineType, TransportEngine]):
    pass


@TransportEngineFactory.register(TransportEngineType.AIOHTTP)
class AiohttpEngine(TransportEngine):
    """
    HttpTransport adapter that uses aiohttp.ClientSession to make HTTP requests.
    """

    def __init__(
        self,
        base_url: str,
        connector_config: TcpConnectionConfig,
        tls_config: TlsConfig | None = None,
        base_timeout: int = 30,
        warmup_timeout: int = 10,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._connector_config = connector_config
        self._tls_config = tls_config
        self._timeout = ClientTimeout(total=base_timeout)
        self._warmup_timeout = ClientTimeout(total=warmup_timeout)

        self._connector: TCPConnector | None = None
        self._session: ClientSession | None = None
        self._warmup_error: str | None = None
        self._warmed_up: bool = False

    @property
    def session(self) -> ClientSession | None:
        if self._session is None:
            raise ValueError(f"{__class__.__name__} aiohttp ClientSession not assigned")
        return self._session

    @session.setter
    def session(self, session: ClientSession | None) -> None:
        self._session = session

    def _build_ssl_context(self, cfg: TlsConfig) -> ssl.SSLContext | None:
        context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

        if not cfg.verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if cfg.ca_bundle:
            context.load_verify_locations(cafile=str(cfg.ca_bundle))

        if cfg.client_cert:
            context.load_cert_chain(
                certfile=str(cfg.client_cert),
                keyfile=str(cfg.client_key) if cfg.client_key else None,
            )

        return context

    def _build_tcp_connector(self, cfg: TcpConnectionConfig) -> TCPConnector:
        kwargs = cfg.model_dump(exclude={"tls"})

        if cfg.tls and cfg.tls.enabled:
            kwargs["ssl"] = self._build_ssl_context(cfg.tls)

        return TCPConnector(**kwargs)

    async def __aenter__(self) -> Self:
        if self._connector is None:
            self._connector = self._build_tcp_connector(self._connector_config)

        self.session = ClientSession(connector=self._connector, timeout=self._timeout)

        return self

    async def __aexit__(
        self,
        exc_type: BaseException | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None

    async def warmup(self) -> None:
        """
        Perform a lightweight connection warm-up against the base URL.

        This primes DNS resolution, TCP connection establishment, TLS
        handshake, and initializes the connection pool.
        """

        if self._session is None:
            self._warmup_error = "HttpTransport session not initialized"
            self._warmup = False
            return

        try:
            async with self._session.get(
                self._base_url,
                timeout=self._warmup_timeout,
            ) as response:
                await (
                    response.release()
                )  # drain response body to ensure connection reuse

            self._warmed_up = True

        except Exception as e:
            self._warmup_error = str(e)
            self._warmed_up = False

    async def send(self, request: TransportRequest) -> TransportResponse:
        if not self.session:
            raise RuntimeError(
                "AiohttpTransport must be used as an async context manager"
            )

        try:
            async with self.session.request(
                request.method,
                request.url,
                headers=request.headers,
                params=request.params,
                json=request.json,
                data=request.data,
            ) as response:
                body = await response.read()
                return TransportResponse(
                    status=response.status,
                    headers=response.headers,
                    body=body,
                    error=None,
                )
        except Exception as e:
            return TransportResponse(
                status=None, headers={}, body=None, error=f"{type(e).__name__}: {e}"
            )


@TransportEngineFactory.register(TransportEngineType.HTTPX)
class HttpxEngine(TransportEngine):
    async def send(self, request: TransportRequest) -> TransportResponse:
        raise NotImplementedError("HttpxTransport has not been implemented")
