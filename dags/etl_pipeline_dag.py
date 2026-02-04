"""
Sample Airflow DAG for the distributed-api-etl pipeline.

This DAG demonstrates how to orchestrate Spark jobs for the ETL pipeline
using the SparkSubmitOperator.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator


# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_dependencies():
    """Verify that required services are available."""
    import socket

    services = [
        ("spark-master", 7077),
        ("hive-metastore", 9083),
        ("minio", 9000),
    ]

    for host, port in services:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            if result != 0:
                raise ConnectionError(f"Cannot connect to {host}:{port}")
        except socket.error as e:
            raise ConnectionError(f"Service {host}:{port} is not available: {e}")

    print("All dependencies are available")


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Distributed API ETL Pipeline",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "spark", "delta"],
) as dag:

    # Task 1: Check dependencies
    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_dependencies,
    )

    # Task 2: Run ETL extraction
    # Note: Update the application path to match your actual ETL script location
    extract_data = SparkSubmitOperator(
        task_id="extract_data",
        application="/opt/airflow/src/main.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        application_args=["--config", "/opt/airflow/src/config/pipeline.yaml"],
        verbose=True,
    )

    # Task 3: Validate data quality
    validate_data = SparkSubmitOperator(
        task_id="validate_data",
        application="/opt/airflow/src/validation.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
        },
        verbose=True,
    )

    # Define task dependencies
    check_deps >> extract_data >> validate_data
