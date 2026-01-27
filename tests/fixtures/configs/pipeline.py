import pytest 
from config.models.pipeline import PipelineConfig
from tests.fixtures.configs.endpoint import valid_endpoint_config
from tests.fixtures.configs.auth import no_auth
from tests.fixtures.configs.transport import aiohttp_config
from config.models.data_contract import EtlTableConfig, BronzeSinkConfig


@pytest.fixture
def minimal_pipeline_config():
    return PipelineConfig(
        endpoint=valid_endpoint_config(),
        transport=aiohttp_config(),
        auth=no_auth(),
        middleware=[],
        tables=EtlTableConfig(
            sink=BronzeSinkConfig(
                name="bronze_api_data",
                namespace="catalog.schema",
            )
        ),
    )

