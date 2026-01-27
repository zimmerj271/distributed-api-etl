from pydantic import BaseModel
from typing import Literal, TypeVar, Generic, Any


T = TypeVar("T", bound=str)


class MiddlewareConfigModel(BaseModel, Generic[T]):
    type: T

    def to_runtime_args(self) -> dict[str, Any]:
        return {}


class SimpleMiddlewareModel(MiddlewareConfigModel):
    """Base middleware configuration"""
    type: Literal[
            "logging",
            "timing",
            "json_body",
            "worker_diag",
    ]

    def to_runtime_args(self) -> dict[str, Any]:
        return super().to_runtime_args()


class RetryMiddlewareModel(MiddlewareConfigModel):
    """Retry middleware configuration"""
    type: Literal["retry"] = "retry"
    max_attempts: int = 10
    retry_status_codes: list[int] = [500, 502, 503, 504, 429]
    base_delay: float = 0.1
    max_delay: float = 2.0

    def to_runtime_args(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "retry_status_codes": self.retry_status_codes,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
        }
