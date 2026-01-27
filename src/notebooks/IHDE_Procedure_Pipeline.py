# Databricks notebook source
import sys
from pathlib import Path

project_root = Path.cwd().parent  

# Add the project root to sys.path
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# COMMAND ----------

from pipeline.orchestrator import run_pipeline

# COMMAND ----------

config_path = "/Workspace/Users/zimmerju@slhs.org/HealthPartners/Databricks/Apps/api_pipeline/src/config/files/ihde_procedure.yml"

# COMMAND ----------

run_pipeline(
    spark=spark,
    config_path=config_path,
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from keystone_dev.test.procedure_test_api_response;
