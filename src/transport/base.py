from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from types import TracebackType
from typing import Any, Mapping 
from dataclasses import dataclass


class TransportEngineType(str, Enum):
    AIOHTTP = "aiohttp"
    HTTPX = "httpx"


@dataclass
class TransportRequest:
    """
    Wire-level HTTP request container for the Transport Layer.
    This data structure allows for the decoupling of the HTTP
    engine from the rest of the pipeline.
    """
    method: str 
    url: str 
    headers: dict[str, str]
    params: dict[str, Any] | None = None
    json: dict[str, Any] | None = None
    data: Any | None = None


@dataclass
class TransportResponse:
    """
    Wire-level HTTP response container for the Transport Layer.
    This data structure allows for the decoupling of the HTTP
    engine from the rest of the pipeline.
    """
    status: int | None
    headers: Mapping[str, str] | None
    body: bytes | None
    error: str | None = None


class TransportEngine(ABC):
    """
    A structural interface that defines a pluggable HTTP engine abstraction.
    The HTTP Transport engine performs a single HTTP request and returns a
    low-level TransportResponse. Implementations may wrap aiohttp, httpx,
    requests, urllib3, etc. Transport is also the lifecycle manager for an
    HTTP session.
    """

    @abstractmethod
    async def __aenter__(self) -> "TransportEngine":
        ...

    @abstractmethod
    async def __aexit__(
        self, 
        exc_type: BaseException | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None,
    ) -> None:
        ...

    @abstractmethod
    async def send(self, request: TransportRequest) -> TransportResponse:
        ...


class SessionEngine(TransportEngine, ABC):

    @property
    @abstractmethod
    def session(self) -> Any: ...
