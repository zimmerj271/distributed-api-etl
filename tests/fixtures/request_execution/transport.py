from types import TracebackType
from typing import Optional
from request_execution.models import TransportRequest, TransportResponse
from request_execution.transport.base import TransportEngine
from config.models.transport import TcpConnectionConfig, TlsConfig


class FakeTransportEngine(TransportEngine):
    def __init__(self, response: TransportResponse):
        self.response = response
        self.last_request: Optional[TransportRequest] = None

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: BaseException | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def send(self, request: TransportRequest) -> TransportResponse:
        self.last_request = request
        return self.response


def tcp_config_no_tls() -> TcpConnectionConfig:
    return TcpConnectionConfig(
        limit=10,
        limit_per_host=2,
        ttl_dns_cache=60,
        force_close=False,
        enable_cleanup_closed=True,
        tls=None,
    )


def tls_config_disabled() -> TlsConfig:
    return TlsConfig(
        enabled=False,
        verify=True,
    )


def sample_transport_response() -> TransportResponse:
    return TransportResponse(
        status=200,
        headers={"Content-Type": "application/json"},
        body=b"{}",
        error=None,
    )

