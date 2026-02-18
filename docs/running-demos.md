# Running the Demo Pipelines

This project includes demo pipelines that showcase different authentication patterns against a mock API. Each demo generates sample data, makes API requests, and writes responses to Delta tables.

## Available Demos

| Demo | Auth Pattern | Config File | Description |
|------|--------------|-------------|-------------|
| No Auth | None | `noauth_demo.yml` | API requests without authentication |
| Basic Auth | HTTP Basic | `basic_auth_demo.yml` | Username/password in Authorization header |
| Bearer Token | Static Bearer | `bearer_token_demo.yml` | Pre-configured API key token |
| OAuth2 Password | OAuth2 ROPC | `oauth2_user_credentials_demo.yml` | User credentials via Keycloak |
| OAuth2 Client Credentials | OAuth2 CC | `oauth2_client_credentials_demo.yml` | Service-to-service via Keycloak |

## Quick Start

```bash
# 1. Build all images needed for demos
make build-demo

# 2. Start platform with all demo services
make up-demo

# 3. Wait for services to be healthy (~60-90 seconds for Keycloak)
make ps

# 4. Run demos via Jupyter or Airflow (see below)
```

## Option 1: Run via Airflow UI

Airflow provides scheduled/triggered execution for production workflows.

1. Open Airflow UI at **http://localhost:8088** (admin/admin)
2. Find the demo DAGs (filter by "demo" tag):
   - `demo_noauth_pipeline`
   - `demo_basic_auth_pipeline`
   - `demo_bearer_token_pipeline`
   - `demo_oauth2_password_pipeline`
   - `demo_oauth2_client_credentials_pipeline`
   - `demo_all_pipelines_parallel` (runs all demos in parallel)
3. Enable the DAG by toggling the switch to "On"
4. Click the play button > "Trigger DAG"
5. Monitor execution in the DAG's Graph or Grid view

## Option 2: Run via Airflow CLI

Run demos directly from the command line:

```bash
# Unpause a DAG (required before first run)
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags unpause demo_noauth_pipeline

# Trigger the DAG
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags trigger demo_noauth_pipeline

# Monitor the run status
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags list-runs -d demo_noauth_pipeline

# Run all demos in parallel
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags unpause demo_all_pipelines_parallel

docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags trigger demo_all_pipelines_parallel
```
Results land in Delta tables viewable via the Spark SQL shell, MinIO console or Jupyter notebook.

## Option 3: Run via Jupyter Notebooks

Jupyter provides an interactive way to run and explore the demos.

1. Open JupyterLab at **http://localhost:8888** (Token: `jupyter`)
2. Navigate to `notebooks/` and open any demo notebook:
   - `noauth_demo.ipynb`
   - `basic_auth_demo.ipynb`
   - `bearer_token_demo.ipynb`
   - `oauth2_password_demo.ipynb`
   - `oauth2_client_credentials_demo.ipynb`
3. Run all cells (Shift+Enter or Run > Run All Cells)
4. View results in the final cells showing:
   - Total records processed
   - Status code distribution
   - Sample response bodies

## Parallel Execution Demo

The `demo_all_pipelines_parallel` DAG runs all 5 demos simultaneously to showcase distributed parallelism:

```
check_dependencies → check_keycloak → ┬─ run_noauth_pipeline
                                      ├─ run_basic_auth_pipeline
                                      ├─ run_bearer_pipeline
                                      ├─ run_oauth2_password_pipeline
                                      └─ run_oauth2_client_credentials_pipeline
```

All 5 Spark jobs are submitted simultaneously and execute across the Spark cluster workers.

## Verifying Results

### Via PySpark Shell
```bash
# Enter the PySpark shell:
make spark-shell

# List available tables (from Hive Metastore)
spark.sql("SHOW TABLES").show()

# Query a Delta table by name
spark.table("demo.noauth_demo_response").show()
```

### Via Spark SQL Shell

```bash
# Enter the Spark SQL shell:
make spark-sql

# In the Spark SQL shell:
SHOW DATABASES;
USE demo;
SHOW TABLES;
SELECT * FROM noauth_demo_response LIMIT 10;
```

### Via MinIO Console

1. Open http://localhost:9001 (minioadmin/minioadmin)
2. Navigate to the `warehouse` bucket
3. Browse to `demo.db/<table_name>/` to see Delta files

### Via Jupyter

```python
# Read the sink table
spark.table("demo.noauth_demo_response").show()

# Check record counts
spark.table("demo.noauth_demo_response").count()

# Inspect response data
spark.table("demo.noauth_demo_response").select("body_text").show(truncate=False)
```

## Troubleshooting

### Services not starting

```bash
# Check service health
make ps

# View logs for a specific service
make logs SERVICE=spark-master
make logs SERVICE=mock-api
make logs SERVICE=keycloak
```

### DAG fails with "connection refused"

- Ensure mock-api is running: `docker compose -f docker/docker-compose.yml --profile keycloak ps`
- Check if using the correct profile: `make up-keycloak` not just `make up`

### OAuth2 demos fail to get token

- Keycloak takes ~60s to start; wait for health check to pass
- Verify Keycloak is accessible: `curl http://localhost:8180/health/ready`

### Tables not found

- The Hive metastore must be healthy before running pipelines
- Check: `make logs SERVICE=hive-metastore`
- Databases are created on first pipeline run

### Checking Spark Driver Logs

When a Spark job fails, check the driver logs:

```bash
# List driver directories
docker exec <spark-worker-container> ls /opt/spark/work/

# Check stdout (Python output)
docker exec <spark-worker-container> cat /opt/spark/work/driver-XXXXXX/stdout

# Check stderr (JVM/Spark logs)
docker exec <spark-worker-container> cat /opt/spark/work/driver-XXXXXX/stderr
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | Token: `jupyter` |
| Airflow | http://localhost:8088 | admin / admin |
| Spark Master UI | http://localhost:8080 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Keycloak Admin | http://localhost:8180 | admin / admin |
| Mock API Health | http://localhost:8200/health | -- |
