import json
import yaml

from config.loader import ConfigLoader
from config.models.pipeline import PipelineConfig
from tests.fixtures.configs.pipeline import minimal_pipeline_config


def test_loader_from_yaml_string():
    loader = ConfigLoader()
    config = minimal_pipeline_config
    yaml_text = yaml.dump(config)

    cfg = loader.from_yaml(yaml_text)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.endpoint.name == "test_endpoint"


def test_loader_from_json_file(tmp_path):
    p = tmp_path / "config.json"
    config = minimal_pipeline_config
    p.write_text(json.dumps(config))

    loader = ConfigLoader()
    cfg = loader.from_json(p)

    assert cfg.endpoint.base_url.startswith("https://")


def test_read_source_prefers_file(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("foo: bar")

    loader = ConfigLoader()
    text = loader._read_source(str(p))

    assert text == "foo: bar"

