import pytest
from config.models.endpoint import EndpointConfigModel
from clients.base import RequestType


@pytest.fixture
def valid_endpoint_config() -> EndpointConfigModel:
    return EndpointConfigModel(
        name="test_endpoint",
        base_url="https://example.com",
        url_path="api/v1/resource",
        method=RequestType.POST,
        headers={"X-Test": "true"},
        vendor="acme",
    )

