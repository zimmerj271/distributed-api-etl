import pytest
from tests.fixtures.configs.pipeline import minimal_pipeline_config


@pytest.mark.unit
@pytest.mark.config
def test_pipeline_defaults(minimal_pipeline_config):
    cfg = minimal_pipeline_config
    assert cfg.execution.batch_size == 10_000
    assert cfg.execution.num_partitions == 200
