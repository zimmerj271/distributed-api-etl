import asyncio
import logging
import socket
import threading
from typing import Coroutine, Any
import aiohttp.web
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

from auth.token.token_manager import TokenManager


class RpcService(ABC):
    """
    Base class for defining a lightweight aiohttp-based RPC service that runs
    on the Spark driver.

    RpcService is responsible for:
      • Discovering a safe host/port on the driver.
      • Defining the aiohttp Application via `build_app()`.
      • Providing a coroutine (`background_coroutine`) that can be run in an
        independent event loop by a generic background runner (e.g.
        AsyncBackgroundService).
      • Exposing a `wait_until_ready` helper to avoid race conditions where
        workers call the service before it is fully started.

    This class is intentionally **not** responsible for managing threads or
    event loops itself. Instead, it focuses on:
      • Constructing the web app and TCP site.
      • Managing aiohttp AppRunner lifecycle.
      • Signaling readiness to callers via a threading.Event.

    In the broader ETL pipeline, RpcService acts as a generic “RPC surface”
    on the driver. Concrete subclasses (such as TokenRpcService) define the
    actual endpoints and semantics. RpcService is also structurall defined as a 
    BackgroundProcess (core.coroutine) by defining a `background_coroutine` which
    is designed to be injected into AsyncBackgroundService, which in turn runs the
    injected function in an independent thread + asyncio event loop, allowing the 
    ETL pipeline to remain synchronous while still hosting asynchronous services 
    on the driver.
    """

    def __init__(
        self,
        spark: SparkSession,
        port: int | None = None 
    ) -> None:
        self.host = spark.sparkContext.getConf().get("spark.driver.host")
        self._logger: logging.Logger = logging.getLogger(f"[{self.__class__.__name__}]")

        if port is not None and not self.valid_port(port):
            raise ValueError(f"Port {port} is not a valid port.")
        self.port = port if port is not None else RpcService.dynamic_port_select()

        self._runner: aiohttp.web.AppRunner | None = None
        self._ready_event = threading.Event()

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @abstractmethod
    def build_app(self) -> aiohttp.web.Application: 
        """Design interface that defines the endpoint for the RPC to be added"""
        ...

    @staticmethod
    def dynamic_port_select() -> int:
        """Fetch an open Port and return"""
        with socket.socket() as sock:
            sock.bind(('', 0))
            port = sock.getsockname()[1]
        return port

    def valid_port(self, port: int) -> bool:
        """
        Verify that port number is not associated to a port already used
        in Spark or Databricks
        """

        restrictedport_ranges: list[tuple[int, int]] = [
            (0, 1023),      # Privileged ports
            (4040, 4050),   # Spark UI
            (8443, 8449),   # Databricks services
            (8649, 8652),   # Ganglia
            (30000, 30010), # Spark shuffle service
        ]

        restrictedport_values: set[int] = {
            22,     # SSH
            80,     # HTTP
            443,    # HTTPS
            53,     # DNS
            1433,   # Databases
            1521,   # Databases
            2200,   # Databricks Connect
            3000,   # Grafana
            5557,   # Databricks internal
            7077,   # Spark Master
            7078,   # Spark Master REST
            8020,   # HDFS NameNode
            8080,   # Spark Master web UI 
            8888,   # Jupyter
            9000,   # HDFS NameNode
            9090,   # Prometheus
            18080,  # Spark history server 
            50070,  # HDFS NameNode web UI
            50075,  # HDFS DataNode web UI
        }

        # if not in range of defined ports
        if not (0 < port <= 65535):
            return False

        return not (
            any(start <= port <= end for start, end in restrictedport_ranges)
            or port in restrictedport_values
        )

    def background_coroutine(self) -> Coroutine[Any, Any, None]:
        """
        Provide the coroutine that keeps the aiohttp server running as a
        background process.
        """
        return self._run_service()

    async def _run_service(self) -> None:
        app = self.build_app()
        if not isinstance(app, aiohttp.web.Application):
            raise TypeError("build_app must return aiohttp.web.Application")

        # Create custom aiohttp.access log format
        access_logger = logging.getLogger(f"[{self.__class__.__name__}.request]")

        runner = aiohttp.web.AppRunner(
            app,
            access_log=access_logger,
                access_log_format="Worker %a → %r | Status: %s | Duration: %Tf seconds"
        )
        await runner.setup()
        self._runner = runner

        site = aiohttp.web.TCPSite(
            runner=self._runner,
            host=self.host,
            port=self.port,
        )
        await site.start()

        self._logger.info(f"Started at {self.url}")
        self._ready_event.set()

        try:
            # Block forever until cancelled
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self._logger.info("Cancel service received, cleaning up...")
            if self._runner is not None:
                await self._runner.cleanup()
            raise

    def wait_until_ready(self, timeout: float | None = 10) -> None:
        """
        A helper function to avoid race conditions by delaying downstream processes
        that may attempt to make requests to the RPC server before it's available.
        """
        if not self._ready_event.wait(timeout=timeout):
            raise TimeoutError("RPC service failed to start.")


class TokenRpcService(RpcService):
    """
    Concrete RpcService that exposes a `/token` endpoint backed by the driver's
    TokenManager.

    TokenRpcService ties the general RPC infrastructure provided by RpcService
    to a specific use case: centralized OAuth2 token distribution. Rather than
    having each Spark worker acquire and refresh its own token from the vendor,
    the driver maintains a single TokenManager and exposes that token via an
    RPC endpoint.

    Responsibilities:
      • Implement `build_app()` to construct an aiohttp Application with a
        GET `/token` route.
      • In the `/token` handler, call `TokenManager.get_token()` and return
        a serialized token (e.g., value + expiry) as JSON.
      • Log and surface errors if the token cannot be retrieved.

    How worker nodes use this:
      • Each worker is configured with an RpcTokenProvider pointed at
        `TokenRpcService.url` (e.g., `http://<driver-host>:<port>`).
      • When a worker needs a bearer token (typically through middleware such
        as WorkerBearerTokenMiddleware), it makes an HTTP GET to `/token`.
      • The worker receives the serialized token and injects an
        `Authorization: Bearer <token>` header for the downstream API call.

    This design ensures:
      • All vendor authentication is centralized on the driver.
      • Worker nodes remain stateless with respect to authentication.
      • Token acquisition/refresh frequency is controlled in one place
        (via TokenManager), reducing the risk of hitting vendor rate limits.
    """

    def __init__(
        self, 
        spark: SparkSession, 
        token_manager: TokenManager,
        port: int | None = None,
    ) -> None:
        super().__init__(spark=spark, port=port)
        self._token_manager = token_manager

    def build_app(self) -> aiohttp.web.Application:
        app = aiohttp.web.Application()

        async def get_token(request: aiohttp.web.Request) -> aiohttp.web.Response:
            try:
                token = await self._token_manager.get_token()
                return aiohttp.web.json_response(token.serialize_token())
            except Exception as e:
                self._logger.error(f"Failed to get token from Token Manager: {e}")
                return aiohttp.web.json_response(
                    {"error": "Failed to retrieve token"},
                    status=500
                )

        app.add_routes([aiohttp.web.get("/token", get_token)])
        return app

