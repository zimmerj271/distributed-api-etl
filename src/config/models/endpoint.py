from typing import Any
from pydantic import Field, field_validator, BaseModel

from request_execution.models import RequestType


class EndpointConfigModel(BaseModel):
    """API endpoint configuration"""
    name: str = Field(..., description="Endpoint identifier")
    base_url: str = Field(..., description="Base URL path")
    url_path: str = Field(..., description="URL path (appended to base_url)")
    method: RequestType = Field(default=RequestType.GET)
    headers: dict[str, str] = Field(default_factory=dict, description="Static headers")
    params: dict[str, str] | None = Field(default_factory=dict, description="Static query params")
    request_template: dict[str, Any] | None = Field(default=None, description="JSON request body template")
    response_schema: str | None = Field(default=None, description="Expected response schema")
    vendor: str | None = Field(default=None, description="Name of the RESTful API vendor")

    @field_validator("url_path")
    @classmethod
    def validate_url_path(cls, v: str) -> str:
        if not v.startswith("/"):
            return f"/{v}"
        return v

