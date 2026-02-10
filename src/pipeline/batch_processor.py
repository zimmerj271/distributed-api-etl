import logging
from math import ceil
from pyspark.sql import functions as F, DataFrame, SparkSession 
from pipeline.base import BatchHandler


class BatchProcessor:
    """
    BatchProcessor

    A reusable batching engine that:
        - Identifies unrpocessed rows (via anti-join on the id column)
        - Creates batches using hash-based grouping
        - Repartitions the DataFrame to avoid AQE collapse
        - Retries failed batches
        - Delegates batch execution to a BatchHandler
    """

    def __init__(
        self,
        spark: SparkSession,
        source_df: DataFrame,
        sink_table: str,
        batch_size: int = 10000,
        max_attempts: int = 5,
        num_partitions: int | None = None
    ) -> None:
        self._spark = spark
        self.src_df = source_df
        self.sink_table = sink_table
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.num_partitions = num_partitions
        self._logger = logging.getLogger(f"[{__class__.__name__}]")

    def __load_remaining(self) -> DataFrame:
        """Determines which rows still need to process"""
        tgt_df = self._spark.table(self.sink_table)

        return self.src_df.join(tgt_df, ["request_id"], "left_anti")

    def __assign_batches(self, df: DataFrame) -> tuple[DataFrame, int]:
        """Assign hash-based batch_ids"""
        total = df.count()
        if total == 0:
            return df.withColumn("batch_id", F.lit(None)), 0 

        num_batches = ceil(total / self.batch_size)
        df = df.withColumn(
            "batch_id",
            (F.hash("request_id") % num_batches)
        )

        return df, num_batches

    def _run_batch(self, df: DataFrame, batch_id: int, handler: BatchHandler) -> None:
        batch_df = df.filter(F.col("batch_id") == batch_id)

        if batch_df.rdd.isEmpty():
            return

        handler.process(batch_df)

    def process(self, handler: BatchHandler) -> None:
        """
        Execute all batches using the provided BatchHandler.
        """
        for attempt in range(1, self.max_attempts + 1):
            remaining = self.__load_remaining()
            if remaining.rdd.isEmpty():
                print("✓ All API requests processed")
                return

            request_df, num_batches = self.__assign_batches(remaining)

            partitions = self.num_partitions or num_batches
            request_df = request_df.repartition(partitions)

            batch_ids = [
                row.batch_id for row in request_df.select("batch_id").distinct().collect()
            ]

            indexed_batches = list(enumerate(batch_ids, start=1))

            self._logger.info(f"➤ Attempt {attempt}: Processing {len(batch_ids)} batches")

            for idx, batch_id in indexed_batches:
                try:
                    self._logger.info(f"    → Processing batch {idx}/{len(indexed_batches)}")
                    self._run_batch(request_df, batch_id, handler)
                except Exception as e:
                    self._logger.info(f"    ✗ Batch {idx}/{len(indexed_batches)} failed: {e}")
                    if attempt == self.max_attempts:
                        raise
                    else:
                        self._logger.info("    ↺ Retrying batch...")
