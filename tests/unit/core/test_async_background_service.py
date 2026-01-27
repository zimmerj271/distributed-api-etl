from core.coroutine import AsyncBackgroundService
from tests.fixtures.core.coroutine import simple_background


def test_background_service_start_and_stop():
    service = AsyncBackgroundService(simple_background)

    assert not service.is_running

    service.start()
    assert service.is_running

    service.stop()
    assert not service.is_running


def test_background_service_start_is_idempotent():
    service = AsyncBackgroundService(simple_background)

    service.start()
    service.start()  # should not raise

    assert service.is_running

    service.stop()


def test_background_service_stop_when_not_running():
    service = AsyncBackgroundService(simple_background)

    service.stop()  # should not raise
    assert not service.is_running

