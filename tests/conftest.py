import pytest
from tests.fixtures.configs.auth import (
    no_auth,
    basic_auth,
    bearer_auth,
    oauth2_password_auth
)
from tests.fixtures.configs.data_contract import simple_table_schema
from tests.fixtures.configs.endpoint import valid_endpoint_config
from tests.fixtures.configs.pipeline import minimal_pipeline_config
from tests.fixtures.configs.transport import aiohttp_config

@pytest.fixture
def dummy_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": "Bearer dummy-token"
    }

