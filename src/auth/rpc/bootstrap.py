import logging
from pyspark.sql import SparkSession
from auth.token.token_manager import DriverTokenManager
from auth.token.token_provider import PasswordGrantTokenProvider
from auth.rpc.service import RpcService, TokenRpcService
from core.coroutine import AsyncBackgroundService, BackgroundProcess


class RpcBootstrapper:
    """
    This class performs all driver-side bootstrap operations to initialize
    the token management framework:

    1. Create a driver side token provision service responsible for fetching the token from the vendor.
    2. Create a token manager responsible for distributing the token.
    3. Start RPC network to pass the token from the driver to the worker nodes.
    4. Start background refresh to refresh the token before token expiration.

    This class wraps the asynchronous coroutines in the token manager and RPC service processes,
    and exposes an entrypoint to start and stop these processes through a simple interface.
    """

    def __init__(
            self, 
            spark: SparkSession, 
            token_url: str,
            credentials: dict[str, str], 
            refresh_margin: int = 60) -> None:
        self._spark = spark
        self._token_url = token_url
        self._credentials = credentials
        self._refresh_margin = refresh_margin 

        self._token_manager: DriverTokenManager | None = None
        self._rpc_service: RpcService | None = None
        self._token_runtime: AsyncBackgroundService | None = None
        self._rpc_runtime: AsyncBackgroundService | None = None
        self._logger = logging.getLogger(f"[{self.__class__.__name__}]")

    @property
    def url(self) -> str:
        if self._rpc_service is None:
            raise ValueError("RPC Service has not been started")
        return self._rpc_service.url

    def start(self) -> None:
        """
        Asynchronous Token system bootstrap.
        1. Create TokenProvider 
        2. Create TokenManager 
        3. Start RPC service (spawns its own thread + event loop)
        4. Start TokenManager background refresh on the RPC event loop
        """

        self._logger.info("Starting RPC token service...")

        provider = PasswordGrantTokenProvider(
            token_url=self._token_url,
            client_id=self._credentials["client_id"],
            client_secret=self._credentials["client_secret"],
            username=self._credentials["username"],
            password=self._credentials["password"],
        )

        token_manager: DriverTokenManager = DriverTokenManager(provider)
        rpc_service: RpcService = TokenRpcService(
            spark=self._spark,
            token_manager=token_manager,
        )

        # Use type checker to enforce protocol compliance
        __ensure_tm_background_process: BackgroundProcess = token_manager
        __ensure_rpc_background_process: BackgroundProcess = rpc_service

        # Initiate Token Service in background coroutine
        token_runtime = AsyncBackgroundService(
            background_fn=token_manager.background_coroutine,
        )

        rpc_runtime = AsyncBackgroundService(
            background_fn=rpc_service.background_coroutine,
        )

        token_runtime.start()
        rpc_runtime.start()
        rpc_service.wait_until_ready(timeout=10)

        self._token_manager = token_manager
        self._rpc_service = rpc_service
        self._token_runtime = token_runtime
        self._rpc_runtime = rpc_runtime

        self._logger.info("TokenManager background refresh started.")
        self._logger.info(f"RPC Token Service running at {self.url}")

    def stop(self) -> None:
        """
        Stop background refresh and stop the RPC service.
        Executed inside a temporary driver-side event loop.
        """

        self._logger.info("Stopping token refresh loop...")
        if self._token_runtime is not None:
            self._token_runtime.stop()

        self._logger.info("Stopping RPC network service...")
        if self._rpc_runtime is not None:
            self._rpc_runtime.stop()

        self._logger.info("Shutdown complete.")

