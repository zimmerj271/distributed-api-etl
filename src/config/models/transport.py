from pathlib import Path
from typing import Any 
from pydantic import Field, BaseModel

from request_execution.transport.base import TransportEngineType


class TlsConfig(BaseModel): 
    enabled: bool = False
    verify: bool = True
    ca_bundle: Path | None = None
    client_cert: Path | None = None
    client_key: Path | None = None


class TcpConnectionConfig(BaseModel):
    limit: int = 100
    limit_per_host: int = 0
    ttl_dns_cache: int = 300
    force_close: bool = False
    enable_cleanup_closed: bool = True
    tls: TlsConfig | None = None


class TransportEngineModel(BaseModel):
    """Base config for transport engine"""
    type: TransportEngineType

    def to_runtime_args(self) -> dict[str, Any]:
        return {}


class AiohttpEngineConfig(TransportEngineModel):
    type: TransportEngineType = Field(default=TransportEngineType.AIOHTTP)
    base_timeout: int
    warmup_timeout: int
    tcp_connection: TcpConnectionConfig = Field(default_factory=TcpConnectionConfig)

    def to_runtime_args(self) -> dict[str, Any]: 
        return {
            "connector_config": self.tcp_connection,
            "base_timeout": self.base_timeout,
            "warmup_timeout": self.warmup_timeout,
        }
