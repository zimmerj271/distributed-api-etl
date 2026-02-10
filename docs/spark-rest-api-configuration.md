# Spark REST API Configuration for Airflow DAGs

This document explains how Spark jobs are submitted via the REST API and why certain configurations must be explicitly passed in the submission payload.

## Background

### Why REST API Instead of SparkSubmitOperator?

Spark Standalone mode has a fundamental limitation: **it does not support cluster deploy mode for Python applications**. This means:

- `SparkSubmitOperator` with `deploy_mode="cluster"` fails with: `Cluster deploy mode is currently not supported for python applications on standalone clusters`
- Only YARN and Kubernetes support cluster deploy mode for Python
- In client mode, the Airflow worker becomes the Spark driver, which defeats the purpose of distributed execution

The Spark REST API provides a workaround by:
1. Submitting jobs directly to the Spark Master's REST endpoint
2. Having the driver run on a Spark worker node
3. Keeping Airflow as a lightweight orchestrator that only submits and monitors jobs

### REST API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `http://spark-master:6066/v1/submissions/create` | POST | Submit a new job |
| `http://spark-master:6066/v1/submissions/status/{submissionId}` | GET | Check job status |
| `http://spark-master:6066/v1/submissions/kill/{submissionId}` | POST | Kill a running job |

## Configuration Architecture

### How spark-submit Works

When using `spark-submit`, the following happens:
1. Spark reads `$SPARK_HOME/conf/spark-defaults.conf`
2. Command-line `--conf` options are merged (overriding defaults)
3. The SparkContext is created with the merged configuration
4. SparkSession inherits from SparkContext

### How REST API Submissions Work

When submitting via the REST API:
1. The Spark Master receives the JSON payload
2. A DriverWrapper JVM is launched on a worker with **only** the `sparkProperties` from the payload
3. `spark-defaults.conf` is **NOT** automatically loaded
4. PythonRunner starts Python and creates a SparkSession
5. The SparkSession only sees properties from the REST payload

This is the critical difference: **REST API submissions bypass spark-defaults.conf entirely**.

## Configuration Split

### What Goes in spark-defaults.conf

The `docker/spark/spark-defaults.conf` file is used for:
- Jupyter notebook sessions
- Direct `spark-submit` CLI usage
- Spark shell sessions

```properties
# Delta Lake
spark.sql.extensions                       io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog            org.apache.spark.sql.delta.catalog.DeltaCatalog

# Hive Metastore
spark.sql.catalogImplementation            hive
spark.hadoop.hive.metastore.uris           thrift://hive-metastore:9083
spark.sql.warehouse.dir                    s3a://warehouse/

# S3/MinIO
spark.hadoop.fs.s3a.endpoint               http://minio:9000
spark.hadoop.fs.s3a.access.key             minioadmin
spark.hadoop.fs.s3a.secret.key             minioadmin
spark.hadoop.fs.s3a.path.style.access      true
spark.hadoop.fs.s3a.impl                   org.apache.hadoop.fs.s3a.S3AFileSystem

# Java 17 Compatibility
spark.driver.extraJavaOptions              --add-opens=java.base/java.lang=ALL-UNNAMED ...
spark.executor.extraJavaOptions            --add-opens=java.base/java.lang=ALL-UNNAMED ...
```

### What Goes in the DAG's SPARK_PROPERTIES

The `dags/demo_pipelines_dag.py` must include all essential configuration because REST API submissions don't load spark-defaults.conf:

```python
SPARK_PROPERTIES = {
    # Submission settings
    "spark.master": SPARK_MASTER_URL,
    "spark.submit.deployMode": "cluster",
    "spark.driver.supervise": "false",

    # Java 17 compatibility (required for JVM startup)
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
```

## Java 17 Compatibility

Spark 3.5.x running on Java 17 requires JVM module access flags. These **must** be present when the JVM starts - they cannot be added later by SparkSession.

```python
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
```

Without these flags, you'll see errors like:
```
java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
cannot access class sun.nio.ch.DirectBuffer (in module java.base) because
module java.base does not export sun.nio.ch to unnamed module
```

## REST API Payload Structure

### PythonRunner Argument Format

When submitting Python applications via REST API, the `appArgs` must follow PythonRunner's expected format:

```python
python_runner_args = [
    app_resource,  # pythonFile - the main Python script
    "",            # pyFiles - empty string if no additional .py/.zip/.egg files
] + app_args       # script arguments (--config, --num-records, etc.)
```

### Complete Payload Example

```python
payload = {
    "appResource": "/opt/spark/app/dags/scripts/run_demo_pipeline.py",
    "sparkProperties": {
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "cluster",
        "spark.app.name": "my_pipeline",
        # ... all other required properties
    },
    "clientSparkVersion": "3.5.1",
    "mainClass": "org.apache.spark.deploy.PythonRunner",
    "environmentVariables": {
        "SPARK_ENV_LOADED": "1",
        "PYTHONPATH": "/opt/spark/app/src",
    },
    "action": "CreateSubmissionRequest",
    "appArgs": [
        "/opt/spark/app/dags/scripts/run_demo_pipeline.py",
        "",
        "--config", "noauth_demo",
        "--num-records", "100",
    ],
}
```

## Enabling the REST API

The Spark Master must have the REST API enabled. In `docker-compose.yml`:

```yaml
spark-master:
  environment:
    - SPARK_MASTER_OPTS=-Dspark.master.rest.enabled=true
  ports:
    - "6066:6066"  # REST API port
```

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG` | Delta Lake config not in payload | Add `spark.sql.extensions` and `spark.sql.catalog.spark_catalog` to SPARK_PROPERTIES |
| `IllegalAccessError: cannot access class sun.nio.ch.DirectBuffer` | Missing Java 17 flags | Add `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` with `--add-opens` flags |
| `error: the following arguments are required: --config` | Wrong PythonRunner arg format | Use `[pythonFile, "", ...args]` format for appArgs |
| Job stays in SUBMITTED state | Worker can't access the Python file | Ensure volumes are mounted correctly in docker-compose |

### Checking Driver Logs

When a job fails, check the driver logs on the worker:

```bash
# Find the latest driver
docker compose exec spark-worker ls -la /opt/spark/work/

# Check stdout (Python output)
docker compose exec spark-worker cat /opt/spark/work/driver-XXXXXX/stdout

# Check stderr (JVM/Spark logs)
docker compose exec spark-worker cat /opt/spark/work/driver-XXXXXX/stderr
```

## Summary

| Aspect | spark-submit | REST API |
|--------|--------------|----------|
| Loads spark-defaults.conf | Yes | No |
| Config source | File + CLI args | Payload only |
| Use case | Interactive, CLI | Programmatic (Airflow) |
| Driver location | Client or cluster | Cluster only |
| Python cluster mode | Not supported | Supported via PythonRunner |

The key takeaway is that REST API submissions require **all** essential Spark configuration to be explicitly included in the `sparkProperties` of the submission payload. The spark-defaults.conf file is still useful for other access methods (Jupyter, spark-shell) but is not used by REST API submissions.
