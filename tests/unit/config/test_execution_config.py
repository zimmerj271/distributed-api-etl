from config.models.execution import ExecutionConfig


def test_execution_defaults():
    cfg = ExecutionConfig()
    assert cfg.num_partitions == 200
    assert cfg.batch_size == 10_000
    assert cfg.max_attempts == 5

