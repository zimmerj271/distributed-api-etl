import pytest
from transport.engine import AiohttpEngine


@pytest.mark.asyncio
async def test_warmup_without_session_sets_error():
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    await engine.warmup()

    assert engine._warmed_up is False
    assert engine._warmup_error is not None


@pytest.mark.asyncio
async def test_warmup_exception_sets_error(monkeypatch):
    engine = AiohttpEngine(
        base_url="https://example.com",
        connector_config=tcp_config_no_tls(),
    )

    class FakeSession:
        async def get(self, *args, **kwargs):
            raise RuntimeError("warmup failed")

    engine.session = FakeSession()

    await engine.warmup()

    assert engine._warmed_up is False
    assert "warmup failed" in engine._warmup_error

