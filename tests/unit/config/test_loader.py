import pytest
import json
import yaml

from config.loader import ConfigLoader
from config.models.pipeline import PipelineConfig
from tests.fixtures.configs.pipeline import minimal_pipeline_config


@pytest.mark.unit
@pytest.mark.config
def test_loader_from_yaml_string(minimal_pipeline_config):
    loader = ConfigLoader()

    # convert PipelineConfig object to a dictionary that can be serialized
    config_dict = minimal_pipeline_config.model_dump(mode="json")
    yaml_text = yaml.dump(config_dict)

    cfg = loader.from_yaml(yaml_text)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.endpoint.name == "test_endpoint"


@pytest.mark.unit
@pytest.mark.config
def test_loader_from_json_file(tmp_path, minimal_pipeline_config):
    p = tmp_path / "config.json"

    config_dict = minimal_pipeline_config.model_dump(mode="json")
    p.write_text(json.dumps(config_dict))

    loader = ConfigLoader()
    cfg = loader.from_json(p)

    assert cfg.endpoint.base_url.startswith("https://")


@pytest.mark.unit
@pytest.mark.config
def test_read_source_prefers_file(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("foo: bar")

    loader = ConfigLoader()
    text = loader._read_source(str(p))

    assert text == "foo: bar"
