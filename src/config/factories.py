from abc import ABC, abstractmethod
from typing import Any, Callable

from config.models.transport import TransportEngineModel
from config.models.middleware import MiddlewareConfigModel
from config.models.endpoint import EndpointConfigModel
from request_execution.transport.base import TransportEngine
from request_execution.transport.engine import AiohttpEngine
from request_execution.middleware.pipeline import (
    MiddlewareFactory, 
    MIDDLEWARE_FUNC,
)
from request_execution.models import (
    RequestContext, 
    RequestMapping, 
    RequestType
)


class RuntimeFactory(ABC):

    @staticmethod
    @abstractmethod
    def build_factory(cfg: Any, *args, **kwargs) -> Callable[[], Any]: ...


class TransportRuntimeFactory(RuntimeFactory):

    @staticmethod
    def build_factory(
        cfg: TransportEngineModel, 
        endpoint_cfg: EndpointConfigModel,
    ) -> Callable[[], TransportEngine]:

        def factory() -> TransportEngine:
            config_kwargs = cfg.to_runtime_args()
            config_kwargs["base_url"] = endpoint_cfg.base_url

            return AiohttpEngine(**config_kwargs)

        return factory


class MiddlewareRuntimeFactory(RuntimeFactory):


    @staticmethod
    def build_factory(cfg: MiddlewareConfigModel) -> Callable[[], MIDDLEWARE_FUNC]:

        def factory() -> MIDDLEWARE_FUNC:
            return MiddlewareFactory.create(cfg.type, **cfg.to_runtime_args())

        return factory

    @staticmethod
    def get_factories(mw_cfgs: list[MiddlewareConfigModel]) -> list[Callable[[], MIDDLEWARE_FUNC]]:

        return [MiddlewareRuntimeFactory.build_factory(cfg) for cfg in mw_cfgs]


class EndpointRuntimeFactory(RuntimeFactory):

    @staticmethod
    def build_request(cfg: EndpointConfigModel, mapping: RequestMapping) -> RequestContext:

        if cfg.method not in RequestType:
            raise ValueError(f"{cfg.method} is not a recognized HTTP protocol")
        method = RequestType[cfg.method]

        metadata = { "vendor": cfg.vendor }

        return RequestContext(
            method=method,
            url=cfg.url_path,
            headers=cfg.headers,
            param_mapping=mapping.param,
            metadata=metadata,
        )

    @staticmethod
    def build_factory(cfg: EndpointConfigModel, mapping: RequestMapping) -> Callable[[], RequestContext]:

        def factory() -> RequestContext:
            return EndpointRuntimeFactory.build_request(cfg, mapping)

        return factory


