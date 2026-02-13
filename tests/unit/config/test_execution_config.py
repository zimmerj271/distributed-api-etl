import pytest
from config.models.execution import ExecutionConfig


@pytest.mark.unit
@pytest.mark.config
def test_execution_defaults():
    cfg = ExecutionConfig()
    assert cfg.num_partitions == 200
    assert cfg.batch_size == 10_000
    assert cfg.max_attempts == 5
    assert cfg.max_concurrent_requests == 20

