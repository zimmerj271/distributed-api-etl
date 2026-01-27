from __future__ import annotations
import hashlib
import json
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field 
from pyspark.sql import Row 
from typing import Any


class RequestType(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


@dataclass(frozen=True)
class RequestMapping:
    """
    Describes how row fields bind to HTTP request parameters.
    This class is intended to be a structural interface for the signature
    of methods in EndpointRuntimeFactory.
    """
    param: dict[str, str]


@dataclass
class RequestContext:
    """
    A container to hold metadata for a HTTP request.
    • method: HTTP request type - GET, POST, etc
    • url: Enpoint URL for HTTP request 
    • auth: A tuple to store the username and password
    • headers: Request headers
    • params: Payload sent as params
    • json: Payload in JSON format
    • data: Payload sent as binary data
    • timeout: Timeout for requests
    • metadata: Metadata associated with the HTTP request
    """
    method: RequestType
    url: str 
    auth: tuple[str, str] = field(default_factory=tuple)
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] | None = None
    param_mapping: dict[str, str] | None = None
    json: dict[str, Any] | None = None
    data: Any | None = None
    timeout: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    _row: Row | None = None

    def with_headers(self, context: "RequestContext", new_headers: dict[str, str]) -> "RequestContext":
        """Return context with headers added"""
        context.headers = context.headers | new_headers
        return context 


@dataclass
class RequestExchange:
    """
    High-level semantic data container of a single HTTP request.
    This container is passed through the Interceptor model as part
    of the ETL Layer which eventually is used to load to a PySpark 
    DataFrame.
    • context: original RequestContext, possibly modified by middleware
    • status: HTTP status response code
    • headers: response headers
    • body: raw response body (bytes), if any
    • json_body: parsed JSON of response
    • success: semantic success flag
    • error_message: error description
    • attempts: how many times the request was attempted (retries)
    • metadata: additional ETL / pipeline metadata
    """
    context: RequestContext
    status_code: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    body_text: Any | None = None
    success: bool = True
    error_message: str | None = None
    attempts: int = 1
    metadata: dict = field(default_factory=dict)

    def build_row(self, request_id_value: str) -> Row:
        """
        Pass RequestResponse data to a PySpark Row object.
        Assumes JsonResponseMiddleware has been used to 
        populate json_body.
        """        
        if self.body is None:
            row_hash = None
        else:
            row_hash = hashlib.sha256(self.body).hexdigest()

        request_time = datetime.now()
        
        return Row(
            request_id=request_id_value,
            row_hash=row_hash,
            url=self.context.url,
            method=str(self.context.method.name),
            request_headers=json.dumps(self.context.headers, default=str),
            request_params=json.dumps(self.context.params, default=str), 
            request_metadata=json.dumps(self.context.metadata, default=str),
            status_code=int(self.status_code) if self.status_code is not None else None,
            response_headers=json.dumps(self.headers, default=str),
            json_body=self.body_text,
            success=bool(self.success),
            error_message=str(self.error_message) if self.error_message else None,
            attempts=int(self.attempts),
            response_metadata=json.dumps(self.metadata, default=str),
            _request_time=request_time
        )
