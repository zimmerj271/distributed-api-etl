import pytest
from config.models.middleware import RetryMiddlewareModel


@pytest.mark.unit
@pytest.mark.config
@pytest.mark.middleware
def test_retry_middleware_runtime_args():
    cfg = RetryMiddlewareModel(max_attempts=3)
    args = cfg.to_runtime_args()
    assert args["max_attempts"] == 3
    assert 500 in args["retry_status_codes"]
