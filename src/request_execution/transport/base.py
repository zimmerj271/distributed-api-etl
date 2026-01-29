from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from types import TracebackType 

from request_execution.models import TransportRequest, TransportResponse


class TransportEngineType(str, Enum):
    AIOHTTP = "aiohttp"
    HTTPX = "httpx"


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
