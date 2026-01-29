from __future__ import annotations
import logging
from typing import Callable
from pyspark.sql import DataFrame, SparkSession
from pathlib import Path

from utils.platform import PlatformDetector
from core.table_manager import TableManager
from core.logging import configure_logging, set_py4j_logging_level
from request_execution.transport.base import TransportEngine
from request_execution.models import RequestContext
from request_execution.middleware.pipeline import MIDDLEWARE_FUNC
from config.models.pipeline import PipelineConfig
from config.models.endpoint import EndpointConfigModel
from config.models.auth import AuthConfigModel
from config.models.middleware import MiddlewareConfigModel
from config.models.execution import ExecutionConfig
from config.models.data_contract import EtlTableConfig, SourceTableConfig
from config.models.transport import TransportEngineModel
from config.loader import ConfigLoader
from config.preprocessor import DatabricksSecretsPreprocessor, DatabricksUtils
from config.factories import (
    TransportRuntimeFactory,
    EndpointRuntimeFactory,
    MiddlewareRuntimeFactory
)
from auth.strategy import AuthRuntime, AuthStrategyFactory
from pipeline.partition_executor import ApiPartitionExecutor
from pipeline.batch_handler import ApiBatchHandler
from pipeline.batch_processor import BatchProcessor


class PipelineOrchestrator:

    def __init__(
        self,
        spark: SparkSession,
        pipeline_configs: PipelineConfig,
        source_id: str,
        source_df: DataFrame | None = None,
    ) -> None:
        self._spark = spark
        self._auth_config: AuthConfigModel = pipeline_configs.auth 
        self._transport_config: TransportEngineModel = pipeline_configs.transport
        self._middleware_configs: list[MiddlewareConfigModel] = pipeline_configs.middleware 
        self._endpoint_config: EndpointConfigModel = pipeline_configs.endpoint
        self._tables_config: EtlTableConfig = pipeline_configs.tables 
        self._execution_config: ExecutionConfig = pipeline_configs.execution
        self.source_df = source_df
        self.source_id = source_id

        self._table_manager = TableManager(spark=spark)
        self._logger = logging.getLogger(f"[{self.__class__.__name__}]")

        self._auth_strategy = AuthStrategyFactory.create(
            key=self._auth_config.type, **self._auth_config.to_runtime_args()
        )

        self._transport_factory: Callable[[], TransportEngine] = (
            TransportRuntimeFactory.build_factory(
                cfg=self._transport_config, 
                endpoint_cfg=self._endpoint_config
            )
        )

        self._endpoint_factory: Callable[[], RequestContext] = (
            EndpointRuntimeFactory.build_factory(
                cfg=self._endpoint_config,
                mapping=self._tables_config.get_request_mapping()
            )
        )

        self._middleware_factories: list[Callable[[], MIDDLEWARE_FUNC]] = (
            MiddlewareRuntimeFactory.get_factories(
                mw_cfgs=self._middleware_configs
            )
        )

    @staticmethod
    def _resolve_source_id_column(
        source_cfg: SourceTableConfig | None,
        source_df: DataFrame | None,
        source_id: str | None,
    ) -> str:
        if source_cfg is not None:
            return source_cfg.id_column

        if source_df is not None:
            if source_id is not None and source_id in source_df.columns:
                return source_id
            raise ValueError(
                "Pipeline invariant violated: unable to resolve source ID column. "
                "Exactly one of `tables.source` or `source_df` must be provided."
            )

        raise RuntimeError("Source data resolution invariance violated")

    @staticmethod
    def _discover_dbutils(spark: SparkSession) -> DatabricksUtils | None:

        # Notebook path
        try:
            import IPython.core.getipython as gipy 
            shell = gipy.get_ipython()
            if shell and "dbutils" in shell.user_ns:
                return shell.user_ns["dbutils"]

        except (ImportError, AttributeError):
            pass

        # Job path / non-notebook path
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(spark)

        except Exception:
            pass

        return None

    @classmethod
    def from_config(
        cls,
        *,
        spark: SparkSession,
        config_path: str | Path,
        source_df: DataFrame | None = None,
        source_id: str | None = None,
    ) -> "PipelineOrchestrator":
        loader = ConfigLoader()

        if PlatformDetector.is_databricks_env():
            dbutils = cls._discover_dbutils(spark)
            if dbutils is None:
                raise RuntimeError(
                    "Running on Databricks but dbutils was not found."
                )
            loader.add_preprocessor(
                DatabricksSecretsPreprocessor(dbutils)
            )

        pipeline_configs = loader.from_yaml(config_path)

        if pipeline_configs.tables.source is None and source_df is None:
            raise ValueError(
                "Missing source input: provide either tables.source in config "
                "or a source_df at runtime."
            )

        if pipeline_configs.tables.source is not None and source_df is not None:
            raise ValueError(
                "Ambiguous source input: provide either tables.source OR source_df, not both."
            )

        id_column = cls._resolve_source_id_column(
            source_cfg=pipeline_configs.tables.source,
            source_df=source_df,
            source_id=source_id,
        )

        return cls(
            spark=spark,
            pipeline_configs=pipeline_configs,
            source_df=source_df,
            source_id=id_column,
        )

    def get_data_source(self) -> DataFrame:
        if self._tables_config.source is not None:
            source = self._tables_config.source.identifier
            return self._spark.table(source)
        if self.source_df is not None:
            return self.source_df

        raise ValueError(
            "Missing source data input: "
            "please assign a source table or provide input DataFrame."
        )

    def _start_runtime_service(self) -> None:

        if isinstance(self._auth_strategy, AuthRuntime):
            self._logger.info("Starting driver-side authentication runtime service")
            self._auth_strategy.runtime_start(self._spark)

            self._logger.info("Adding authentication middleware")
            self._middleware_factories = (
                self._middleware_factories + 
                self._auth_strategy.get_middleware_factories()
            )
        else:
            self._logger.info("Authentication does not have a runtime service... skipping")

    def run(self) -> None:
        configure_logging()
        set_py4j_logging_level()

        self._start_runtime_service()

        self._logger.info("====== Executing Pipeline ======")
        self._logger.info(f"Request from URL: {self._endpoint_config.url_path}")

        src_df = self.get_data_source()
        if self._tables_config.source is not None:
            is_valid, errors = self._tables_config.source.validate_dataframe(src_df)
            if not is_valid:
                ve = ValueError(f"Source validation failed: {errors}")
                self._logger.error(str(ve))
                raise ve

        self._table_manager.create_table(self._tables_config.sink)

        # incoming data must align to downstream use of column name 'request_id'
        src_df = src_df.withColumnRenamed(existing=self.source_id, new="request_id")

        executor = ApiPartitionExecutor(
            transport_factory=self._transport_factory,
            endpoint_factory=self._endpoint_factory,
            middleware_factories=self._middleware_factories,
        )

        handler = ApiBatchHandler(
            spark=self._spark,
            executor=executor,
            sink_table=self._tables_config.sink.identifier,
        )

        processor = BatchProcessor(
            spark=self._spark,
            source_df=src_df,
            sink_table=self._tables_config.sink.identifier,
            batch_size=self._execution_config.batch_size,
            num_partitions=self._execution_config.num_partitions,
            max_attempts=self._execution_config.max_attempts,
        )

        try:
            processor.process(handler)
        finally:
            if isinstance(self._auth_strategy, AuthRuntime):
                self._auth_strategy.runtime_stop()
            self._logger.info(f"Pipeline run finished")


def run_pipeline(
    *,
    spark: SparkSession,
    config_path: str | Path,
    source_df: DataFrame | None = None,
    source_id: str | None = None,
) -> None:
    """
    Simple wrapper interface to expose only necessary parameters to the end-user.
    """
    PipelineOrchestrator.from_config(
        spark=spark,
        config_path=config_path,
        source_df=source_df,
        source_id=source_id,
    ).run()
