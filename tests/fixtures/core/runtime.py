from types import TracebackType
from typing_extensions import Self
from core.runtime import AsyncProcess


class DummyAsyncResource(AsyncProcess):
    def __init__(self):
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> Self:
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: BaseException | None,
        exc: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.exited = True
