import pytest
from config.preprocessor import (
    ConfigValue,
    DatabricksSecretsPreprocessor,
    DatabricksUtils,
    SecretsClient,
)


class FakeSecrets(SecretsClient):
    def get(self, *, scope: str, key: str) -> str:
        return f"resolved:{scope}:{key}"


class FakeDbutils(DatabricksUtils):
    def __init__(self) -> None:
        self.secrets = FakeSecrets()


@pytest.mark.unit
@pytest.mark.config
def test_secret_replacement_scalar():
    pre = DatabricksSecretsPreprocessor(FakeDbutils())

    result = pre.process("{{secret.my_scope:my_key}}")

    assert result == "resolved:my_scope:my_key"


@pytest.mark.unit
@pytest.mark.config
def test_secret_replacement_nested():
    pre = DatabricksSecretsPreprocessor(FakeDbutils())

    data: ConfigValue = {"auth": {"password": "{{secret.auth:pwd}}"}}

    result = pre.process(data)

    assert isinstance(result, dict)

    auth = result["auth"]
    assert isinstance(auth, dict)

    password = auth["password"]
    assert isinstance(password, str)

    assert password == "resolved:auth:pwd"
