"""
Airflow DAGs for running demo authentication pipelines.

This module provides DAGs for running demo pipelines that showcase
different authentication patterns:
- No Authentication
- Basic Authentication
- Static Bearer Token
- OAuth2 Password Grant
- OAuth2 Client Credentials

Individual DAGs run each pipeline separately. The `demo_all_pipelines_parallel`
DAG runs all pipelines simultaneously to showcase distributed parallelism.

Each task submits a Spark job via the Spark Standalone REST API.
The driver runs on the Spark cluster, not in Airflow.
"""

import time
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException


# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Spark REST API configuration
SPARK_MASTER_REST_URL = "http://spark-master:6066"
SPARK_MASTER_URL = "spark://spark-master:7077"

# JVM options for Java 17+ compatibility
JAVA17_OPTS = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
])

# Spark configuration for REST API job submission
# Note: REST API submissions bypass spark-submit, so spark-defaults.conf is NOT
# automatically loaded. All essential Spark config must be passed in the payload.
SPARK_PROPERTIES = {
    # Submission settings
    "spark.master": SPARK_MASTER_URL,
    "spark.submit.deployMode": "cluster",
    "spark.driver.supervise": "false",
    # Java 17 compatibility
    "spark.driver.extraJavaOptions": JAVA17_OPTS,
    "spark.executor.extraJavaOptions": JAVA17_OPTS,
    # Delta Lake (required for Delta table operations)
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Hive Metastore (required for table registration)
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.sql.warehouse.dir": "s3a://warehouse/",
    # S3/MinIO (required for data storage)
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}


def submit_spark_job(
    app_resource: str,
    app_args: list[str],
    app_name: str,
    extra_spark_props: dict[str, str] | None = None,
) -> str:
    """Submit a Spark job via REST API and return the submission ID."""
    url = f"{SPARK_MASTER_REST_URL}/v1/submissions/create"

    spark_props = SPARK_PROPERTIES.copy()
    spark_props["spark.app.name"] = app_name

    # Apply any extra/override properties
    if extra_spark_props:
        spark_props.update(extra_spark_props)

    # For Python apps via REST API with PythonRunner (Spark 3.x):
    # PythonRunner.main expects: [pythonFile, pyFiles, ...scriptArgs]
    # - pythonFile: the main Python file to execute
    # - pyFiles: comma-separated list of .py/.zip/.egg files (empty string if none)
    # - scriptArgs: arguments for the Python script
    python_runner_args = [
        app_resource,           # pythonFile
        "",                     # pyFiles (empty - no additional Python files)
    ] + app_args                # script arguments

    payload = {
        "appResource": app_resource,
        "sparkProperties": spark_props,
        "clientSparkVersion": "3.5.1",
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "environmentVariables": {
            "SPARK_ENV_LOADED": "1",
            "PYTHONPATH": "/opt/spark/app/src",
        },
        "action": "CreateSubmissionRequest",
        "appArgs": python_runner_args,
    }

    print(f"Submitting Spark job: {app_name}")
    print(f"App resource: {app_resource}")
    print(f"App args: {app_args}")

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    result = response.json()
    print(f"Submission response: {result}")

    if not result.get("success"):
        raise AirflowException(f"Failed to submit Spark job: {result.get('message', 'Unknown error')}")

    submission_id = result.get("submissionId")
    if not submission_id:
        raise AirflowException("No submission ID returned from Spark REST API")

    print(f"Job submitted successfully. Submission ID: {submission_id}")
    return submission_id


def wait_for_job_completion(
    submission_id: str,
    poll_interval: int = 10,
    timeout: int = 600,
) -> dict[str, Any]:
    """Poll for job completion and return the final status."""
    url = f"{SPARK_MASTER_REST_URL}/v1/submissions/status/{submission_id}"
    start_time = time.time()

    print(f"Waiting for job {submission_id} to complete...")

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise AirflowException(f"Job {submission_id} timed out after {timeout} seconds")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        result = response.json()
        driver_state = result.get("driverState", "UNKNOWN")

        print(f"Job {submission_id} state: {driver_state} (elapsed: {int(elapsed)}s)")

        if driver_state in ("FINISHED",):
            print(f"Job {submission_id} completed successfully")
            return result
        elif driver_state in ("FAILED", "ERROR", "KILLED", "RELAUNCHING"):
            raise AirflowException(f"Job {submission_id} failed with state: {driver_state}")
        elif driver_state in ("SUBMITTED", "RUNNING"):
            time.sleep(poll_interval)
        else:
            # Unknown state, keep polling
            time.sleep(poll_interval)


def run_spark_pipeline(
    config_name: str,
    num_records: int = 100,
    driver_memory: str | None = None,
    executor_memory: str | None = None,
    **context,
) -> None:
    """Submit and monitor a Spark pipeline job.

    Args:
        config_name: Name of the pipeline config file (without .yml extension)
        num_records: Number of records to generate for the demo
        driver_memory: Override driver memory (e.g., "512m", "1g")
        executor_memory: Override executor memory (e.g., "512m", "1g")
    """
    app_name = f"{config_name}_pipeline"
    # Path without file: prefix for PythonRunner
    app_resource = "/opt/spark/app/dags/scripts/run_demo_pipeline.py"
    app_args = [
        "--config", config_name,
        "--num-records", str(num_records),
        "--config-dir", "/opt/spark/app/configs/examples",
    ]

    # Build extra properties for memory overrides
    extra_props = {}
    if driver_memory:
        extra_props["spark.driver.memory"] = driver_memory
    if executor_memory:
        extra_props["spark.executor.memory"] = executor_memory

    # Submit the job
    submission_id = submit_spark_job(
        app_resource=app_resource,
        app_args=app_args,
        app_name=app_name,
        extra_spark_props=extra_props if extra_props else None,
    )

    # Wait for completion
    result = wait_for_job_completion(submission_id)

    print(f"Pipeline {config_name} completed: {result}")


def check_services():
    """Verify that required services are available."""
    import socket

    services = [
        ("spark-master", 7077),
        ("spark-master", 6066),  # REST API
        ("hive-metastore", 9083),
        ("minio", 9000),
        ("mock-api", 8000),
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

    print("All required services are available")


def check_keycloak():
    """Verify that Keycloak is available (for OAuth2 demos)."""
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex(("keycloak", 8080))
        sock.close()
        if result != 0:
            raise ConnectionError("Keycloak is not available on port 8080")
    except socket.error as e:
        raise ConnectionError(f"Keycloak is not available: {e}")

    print("Keycloak is available")


# DAG 1: No Authentication Demo
with DAG(
    dag_id="demo_noauth_pipeline",
    default_args=default_args,
    description="Demo pipeline - No Authentication",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "noauth", "etl"],
) as noauth_dag:

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    run_pipeline = PythonOperator(
        task_id="run_noauth_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={"config_name": "noauth_demo"},
    )

    check_deps >> run_pipeline


# DAG 2: Basic Authentication Demo
with DAG(
    dag_id="demo_basic_auth_pipeline",
    default_args=default_args,
    description="Demo pipeline - Basic Authentication",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "basic-auth", "etl"],
) as basic_auth_dag:

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    run_pipeline = PythonOperator(
        task_id="run_basic_auth_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={"config_name": "basic_auth_demo"},
    )

    check_deps >> run_pipeline


# DAG 3: Static Bearer Token Demo
with DAG(
    dag_id="demo_bearer_token_pipeline",
    default_args=default_args,
    description="Demo pipeline - Static Bearer Token",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "bearer", "etl"],
) as bearer_dag:

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    run_pipeline = PythonOperator(
        task_id="run_bearer_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={"config_name": "bearer_token_demo"},
    )

    check_deps >> run_pipeline


# DAG 4: OAuth2 Password Grant Demo
with DAG(
    dag_id="demo_oauth2_password_pipeline",
    default_args=default_args,
    description="Demo pipeline - OAuth2 Password Grant",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "oauth2", "password-grant", "etl"],
) as oauth2_password_dag:

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    check_kc = PythonOperator(
        task_id="check_keycloak",
        python_callable=check_keycloak,
    )

    run_pipeline = PythonOperator(
        task_id="run_oauth2_password_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={"config_name": "oauth2_user_credentials_demo"},
    )

    check_deps >> check_kc >> run_pipeline


# DAG 5: OAuth2 Client Credentials Demo
with DAG(
    dag_id="demo_oauth2_client_credentials_pipeline",
    default_args=default_args,
    description="Demo pipeline - OAuth2 Client Credentials",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "oauth2", "client-credentials", "etl"],
) as oauth2_cc_dag:

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    check_kc = PythonOperator(
        task_id="check_keycloak",
        python_callable=check_keycloak,
    )

    run_pipeline = PythonOperator(
        task_id="run_oauth2_client_credentials_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={"config_name": "oauth2_client_credentials_demo"},
    )

    check_deps >> check_kc >> run_pipeline


# DAG 6: All Demos in Parallel
# This DAG runs all demo pipelines simultaneously to showcase distributed parallelism
# Uses reduced memory settings to allow 5 concurrent jobs on limited resources
PARALLEL_DRIVER_MEMORY = "512m"
PARALLEL_EXECUTOR_MEMORY = "512m"

with DAG(
    dag_id="demo_all_pipelines_parallel",
    default_args=default_args,
    description="Run all demo pipelines in parallel to showcase distributed execution",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "parallel", "all", "etl"],
) as all_demos_dag:

    # Check all dependencies first
    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_services,
    )

    check_kc = PythonOperator(
        task_id="check_keycloak",
        python_callable=check_keycloak,
    )

    # Create pipeline tasks for each demo with reduced memory for parallel execution
    run_noauth = PythonOperator(
        task_id="run_noauth_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={
            "config_name": "noauth_demo",
            "driver_memory": PARALLEL_DRIVER_MEMORY,
            "executor_memory": PARALLEL_EXECUTOR_MEMORY,
        },
    )

    run_basic_auth = PythonOperator(
        task_id="run_basic_auth_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={
            "config_name": "basic_auth_demo",
            "driver_memory": PARALLEL_DRIVER_MEMORY,
            "executor_memory": PARALLEL_EXECUTOR_MEMORY,
        },
    )

    run_bearer = PythonOperator(
        task_id="run_bearer_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={
            "config_name": "bearer_token_demo",
            "driver_memory": PARALLEL_DRIVER_MEMORY,
            "executor_memory": PARALLEL_EXECUTOR_MEMORY,
        },
    )

    run_oauth2_password = PythonOperator(
        task_id="run_oauth2_password_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={
            "config_name": "oauth2_user_credentials_demo",
            "driver_memory": PARALLEL_DRIVER_MEMORY,
            "executor_memory": PARALLEL_EXECUTOR_MEMORY,
        },
    )

    run_oauth2_client_creds = PythonOperator(
        task_id="run_oauth2_client_credentials_pipeline",
        python_callable=run_spark_pipeline,
        op_kwargs={
            "config_name": "oauth2_client_credentials_demo",
            "driver_memory": PARALLEL_DRIVER_MEMORY,
            "executor_memory": PARALLEL_EXECUTOR_MEMORY,
        },
    )

    # Dependencies check first, then keycloak, then all pipelines run in parallel
    check_deps >> check_kc >> [
        run_noauth,
        run_basic_auth,
        run_bearer,
        run_oauth2_password,
        run_oauth2_client_creds,
    ]