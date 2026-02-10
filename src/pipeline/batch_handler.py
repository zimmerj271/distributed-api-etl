import logging 
from pyspark.sql import types as T, SparkSession, DataFrame

from pipeline.base import BatchHandler, PartitionExecutor


class ApiBatchHandler(BatchHandler):
    """
    ApiBatchHandler

    Applies the ApiPartitionExecutor to a DataFrame batch, converts the results
    back to a DataFrame, and writes the results to a Delta table.

    This class is job-specific but fully reusable for any endpoint strategy.
    """

    def __init__(
        self,
        spark: SparkSession,
        executor: PartitionExecutor,
        sink_table: str
    ) -> None:
        self._spark = spark
        self.executor = executor
        self.sink_table = sink_table
        self._logger = logging.getLogger(f"[{__class__.__name__}]")

    def __get_target_schema(self) -> T.StructType:
        try:
            return self._spark.table(self.sink_table).schema
        except Exception as e:
            self._logger.error(f"Could not get table schema for table {self.sink_table}: {e}")
            raise

    def process(self, batch_df: DataFrame) -> None:
        """
        Process the partition function provided by the partition executor
        and write the results to the target Delta table.
        """
        fn = self.executor.make_map_partitions_fn()
        rdd = batch_df.rdd.mapPartitions(fn)
        schema = self.__get_target_schema()

        result_df = self._spark.createDataFrame(rdd, schema=schema)


        result_df.write.format("delta").mode("append").saveAsTable(self.sink_table)

