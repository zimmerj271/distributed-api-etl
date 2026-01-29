import pytest
from .fixtures.configs.auth import(
    no_auth,
    basic_auth,
    bearer_auth,
    oauth2_password_auth,
)
from .fixtures.configs.data_contract import simple_table_schema
from .fixtures.configs.endpoint import valid_endpoint_config
from .fixtures.configs.pipeline import minimal_pipeline_config
from .fixtures.configs.transport import aiohttp_config

@pytest.fixture
def dummy_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer dummy-token"
    }

__all__ = [
    'no_auth',
    'basic_auth',
    'bearer_auth',
    'oauth2_password_auth',
    'simple_table_schema',
    'valid_endpoint_config',
    'minimal_pipeline_config',
    'aiohttp_config',
]
