import pytest
from config.models.endpoint import EndpointConfigModel


@pytest.mark.unit
@pytest.mark.config
def test_url_path_is_normalized():
    cfg = EndpointConfigModel(
        name="test",
        base_url="https://example.com",
        url_path="v1/resource",
    )
    assert cfg.url_path == "/v1/resource"


@pytest.mark.unit
@pytest.mark.config
def test_url_path_keeps_leading_slash():
    cfg = EndpointConfigModel(
        name="test",
        base_url="https://example.com",
        url_path="/v1/resource",
    )
    assert cfg.url_path == "/v1/resource"

