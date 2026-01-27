from __future__ import annotations
import re
from abc import ABC, abstractmethod
from typing import Protocol, TypeAlias


ConfigScalar: TypeAlias = str   # extend to str | int | float | bool as needed
ConfigValue: TypeAlias = (
    ConfigScalar | dict[str, "ConfigValue"] | list["ConfigValue"]
)


class ConfigPreprocessor(ABC):
    """
    Transforms raw config structures (dict/list/str) BEFORE pydantic validation.

    Examples:
        - Resolve {{secrets.scope:key}} placeholders
        - Resolve ${ENV_VAR} placeholders
        - Apply overlays (base.yaml + env.yaml)
        - Fill derived defaults that are easier to pre-parse
    """

    @abstractmethod
    def process(self, data: ConfigValue) -> ConfigValue: ...


class SecretsClient(Protocol):
    def get(self, *, scope: str, key: str) -> str: ...


class DatabricksUtils(Protocol):
    secrets: SecretsClient


class DatabricksSecretsPreprocessor(ConfigPreprocessor):
    """
    Resolves {{secret.<scope>:<key>}} placeholders using dbutils.secrets.get
    """

    def __init__(self, dbutils: DatabricksUtils):
        self._dbutils = dbutils
        self._secret_pattern = re.compile(
            r"\{\{\s*secret\.([a-zA-Z0-9_-]+):([a-zA-Z0-9_-]+)\s*\}\}"
        )

    def _replace_secret(self, s: str) -> str:
        def replace(m: re.Match) -> str:
            scope, key = m.groups()
            return self._dbutils.secrets.get(scope=scope, key=key)

        return self._secret_pattern.sub(replace, s)

    def process(self, data: ConfigValue) -> ConfigValue:
        if isinstance(data, dict):
            return {k: self.process(v) for k, v in data.items()}

        if isinstance(data, list):
            return [self.process(v) for v in data]

        if isinstance(data, str):
            return self._replace_secret(data)
        
        return data
