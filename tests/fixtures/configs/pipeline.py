import pytest 
from config.models.pipeline import PipelineConfig
from config.models.data_contract import EtlTableConfig, BronzeSinkConfig


@pytest.fixture
def minimal_pipeline_config(valid_endpoint_config, aiohttp_config, no_auth):
    return PipelineConfig(
        endpoint=valid_endpoint_config,
        transport=aiohttp_config,
        auth=no_auth,
        middleware=[],
        tables=EtlTableConfig(
            sink=BronzeSinkConfig(
                name="bronze_api_data",
                namespace="catalog.schema",
            )
        ),
    )

