from pydantic import BaseModel, Field


class ExecutionConfig(BaseModel):
    """
    Controls Spark execution and pipeline batching behavior.
    """
    num_partitions: int = Field(
        default=200, 
        description="Number of partitions to use for API execution"
    )
    batch_size: int = Field(
        default=10_000,
        description="Number of rows per batch"
    )
    max_attempts: int = Field(
        default=5,
        description="Number of pipeline retry attempts before failure"
    )
