from pydantic import Field
from config.models.execution import ExecutionConfig
from config.models.transport import AiohttpEngineConfig
from config.models.auth import AuthConfigUnion
from config.models.endpoint import EndpointConfigModel
from config.models.middleware import MiddlewareConfigModel
from config.models.data_contract import EtlTableConfig


from pydantic import BaseModel


class PipelineConfig(BaseModel):
    """
    Complete pipeline configuration that can be loaded from JSON or YAML.
    """
    endpoint: EndpointConfigModel 
    transport: AiohttpEngineConfig
    auth: AuthConfigUnion
    middleware: list[MiddlewareConfigModel]
    tables: EtlTableConfig
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
