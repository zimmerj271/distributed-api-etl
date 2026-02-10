import os
from pyspark.sql import SparkSession


class PlatformDetector:

    @staticmethod
    def is_databricks_env() -> bool:
        return any(
            key in os.environ
            for key in (
                "DATABRICKS_RUNTIME_VERSION",
                "DATABRICKS_CLUSTER_ID",
                "DATABRICKS_WORKSPACE_URL",
                "DB_HOME",
            )
        )

    @staticmethod
    def is_databricks_spark(spark: SparkSession) -> bool:
        try:
            conf = spark.sparkContext.getConf()
            return conf.contains("spark.databricks.clusterUsageTags.clusterId")
        except Exception:
            return False
