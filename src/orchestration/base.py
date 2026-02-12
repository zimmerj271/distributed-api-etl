from typing import Protocol, Callable, Iterable
from pyspark.sql import DataFrame, Row


class PartitionExecutor(Protocol):
    """
    A structural interface to define objects that execute API calls per Spark partition.
    """

    def make_map_partitions_fn(self, *args, **kwargs) -> Callable[[Iterable[Row]], Iterable[Row]]: ...


class Pipeline(Protocol):

    def execute(self) -> None: ...


class BatchHandler(Protocol):
    """
    A lightweight interface representing the processing of a single batch 
    of a Spark DataFrame. This is intentionally minimal and contains no batch 
    orchestration logic - just the transformation of the batch dataframe into 
    its final written form.

    BatchProcessors call this method for each batch_id.
    """

    def process(self, batch_df: DataFrame, *args, **kwargs) -> None:
        """
        Process a single batch of data.
        Parameters:
            batch_df: DataFrame
                The Spark DataFrame representing a single batch of unprocessed rows.

        Returns:
            None
        """
        ...
