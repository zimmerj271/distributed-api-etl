# Databricks notebook source
import sys
from pathlib import Path

project_root = Path.cwd().parent  

# Add the project root to sys.path
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# COMMAND ----------

from pyspark.sql import functions as F
from orchestration.orchestrator import run_pipeline

# COMMAND ----------

config_path = "/Workspace/Users/zimmerju@slhs.org/HealthPartners/Databricks/Apps/api_pipeline/src/config/files/naas_no_demo.yml"

# COMMAND ----------

df = (
    spark.range(100)
         .repartition(4)
         .select(
             F.sha2(F.uuid(), 256).alias("tracking_id"),
            #  F.lit("").cast("string").alias("dummy"),
         )
)

df.display()

# COMMAND ----------

run_pipeline(
    spark=spark,
    config_path=config_path,
    source_df=df,
    source_id="tracking_id"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from keystone_dev.test.naas_api_response
