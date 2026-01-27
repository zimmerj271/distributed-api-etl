from tests.fixtures.configs.pipeline import minimal_pipeline_config


def test_pipeline_defaults():
    cfg = minimal_pipeline_config
    assert cfg.execution.batch_size == 10_000
    assert cfg.execution.num_partitions == 200
