import json
import yaml 
from typing import Any, Callable
from pathlib import Path

from config.models.pipeline import PipelineConfig
from config.preprocessor import ConfigPreprocessor, ConfigValue


class ConfigLoader:
    """
    Load + preprocess + validate pipeline configs from YAML/JSON.

    - Preprocessors run on raw data before Pydantic validation.
    - Result is fully validated PipelineConfig
    """

    def __init__(self, preprocessors: list[ConfigPreprocessor] | None = None):
        self._preprocessors = preprocessors or []

    def add_preprocessor(self, preprocessor: ConfigPreprocessor) -> None:
        self._preprocessors.append(preprocessor)


    def from_yaml(self, source: str | Path) -> PipelineConfig:
        data = self._load(source, parser=yaml.safe_load)
        return self._build(data)

    def from_json(self, source: str | Path) -> PipelineConfig:
        data = self._load(source, parser=json.loads)
        return self._build(data)

    def _load(
        self,
        source: str | Path,
        *,
        parser: Callable[[str], Any],
    ) -> ConfigValue:
        """
        Load config from a file path or raw string, then parse.
        """
        text = self._read_source(source)
        return parser(text)

    def _read_source(self, source: str | Path) -> str:
        """
        Read source as text.
        If `source` is a file path, read it.
        Otherwise treat it as raw content.
        """
        if isinstance(source, Path):
            return source.read_text()

        # string: path or raw content?
        p = Path(source)
        if p.exists():
            return p.read_text()

        return source

    def _build(self, data: ConfigValue) -> PipelineConfig:
        """
        Apply preprocessors and validate into PipelineConfig.
        """
        for pre in self._preprocessors:
            data = pre.process(data)

        return PipelineConfig.model_validate(data)
