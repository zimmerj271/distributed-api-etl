# Running the Demo Pipelines

This project includes demo pipelines that showcase different authentication patterns. Each demo generates sample data, makes one API request per row, and writes the responses to a Delta table in the `demo` database.

## Available Demos

| Demo | Auth Pattern | Config File | Notebook | Airflow DAG |
|------|--------------|-------------|----------|-------------|
| No Auth | None | `noauth_demo.yml` | `noauth_demo.ipynb` | `demo_noauth_pipeline` |
| Basic Auth | HTTP Basic | `basic_auth_demo.yml` | `basic_auth_demo.ipynb` | `demo_basic_auth_pipeline` |
| Bearer Token | Static Bearer | `bearer_token_demo.yml` | `bearer_token_demo.ipynb` | `demo_bearer_token_pipeline` |
| OAuth2 Password | OAuth2 ROPC (user credentials via Keycloak) | `oauth2_user_credentials_demo.yml` | `oauth2_user_credentials_demo.ipynb` | `demo_oauth2_password_pipeline` |
| OAuth2 Client Credentials | OAuth2 CC (service-to-service via Keycloak) | `oauth2_client_credentials_demo.yml` | `oauth2_client_credentials_demo.ipynb` | `demo_oauth2_client_credentials_pipeline` |
| NaaS (public API) | None | `naas_demo.yml` | `naas_demo.ipynb` | — (Jupyter only) |

Config files are in `configs/examples/`, notebooks in `notebooks/`.

The first five demos call the local mock API (and Keycloak, for OAuth2). The **NaaS demo** calls a public API on the internet (`https://naas.isalman.dev`), so it needs internet access from the Jupyter container. It has no Airflow DAG, and it writes to the `naas` database instead of `demo`.

The `demo_all_pipelines_parallel` DAG runs the five local demos at the same time (see [Parallel Execution Demo](#parallel-execution-demo)).

## Quick Start

```bash
# 1. Build all images needed for demos
make build-demo

# 2. Start platform with all demo services (Jupyter, Keycloak, mock API)
make up-demo

# 3. Wait until every service shows (healthy) (~1-2 minutes; Keycloak is usually last)
make ps

# 4. Run demos via Airflow or Jupyter (see below)
```

See [Check that it's healthy](../README.md#3-check-that-its-healthy) in the README for what `make ps` should show.

## Option 1: Run via Airflow UI

Airflow submits each demo to the Spark cluster in cluster mode: the driver and executors run on the Spark workers.

1. Open Airflow UI at **http://localhost:8088** (admin/admin)
2. Find the demo DAGs (filter by the "demo" tag):
   - `demo_noauth_pipeline`
   - `demo_basic_auth_pipeline`
   - `demo_bearer_token_pipeline`
   - `demo_oauth2_password_pipeline`
   - `demo_oauth2_client_credentials_pipeline`
   - `demo_all_pipelines_parallel` (runs the five demos in parallel)
3. Enable the DAG by toggling the switch to "On"
4. Click the play button > "Trigger DAG"
5. Monitor execution in the DAG's Graph or Grid view

## Option 2: Run via the command line

The Makefile wraps the Airflow CLI:

```bash
# List the DAGs
make dag-list

# Unpause and trigger a DAG
make dag-trigger DAG=demo_noauth_pipeline

# Check its recent runs (state: queued, running, success, or failed)
make dag-status DAG=demo_noauth_pipeline

# Show the state of each task in a run (run_id from the previous command)
make dag-status DAG=demo_noauth_pipeline RUN=<run_id>

# Run the five demos in parallel
make dag-trigger DAG=demo_all_pipelines_parallel

# Latest run of every DAG
make dag-status
```

A single demo takes about a minute. While it runs, the Spark master UI (http://localhost:8080) shows the driver under *Running Drivers* and the application under *Running Applications*.

> If you call `docker compose` directly instead of using the Makefile, always pass the project name:
> `docker compose -f docker/docker-compose.yml -p distributed-api-etl ...`. Without `-p`, Compose
> derives the project name from the `docker/` directory and can't find the running containers.

## Option 3: Run via Jupyter Notebooks

Jupyter provides an interactive way to run and explore the demos.

1. Open JupyterLab at **http://localhost:8888** (Token: `jupyter`)
2. Navigate to `notebooks/` and open any demo notebook:
   - `noauth_demo.ipynb`
   - `basic_auth_demo.ipynb`
   - `bearer_token_demo.ipynb`
   - `oauth2_user_credentials_demo.ipynb`
   - `oauth2_client_credentials_demo.ipynb`
   - `naas_demo.ipynb` (needs internet access)
3. Run all cells (Run > Run All Cells)
4. View results in the final cells showing:
   - Total records processed
   - Status code distribution
   - Sample response bodies

> **The notebooks don't use the Spark cluster.** They run Spark in local mode inside the Jupyter
> container, because the Jupyter image's Spark and Python versions don't match the cluster's.
> They run the same pipeline code and write to the same tables. To see work distributed across
> the workers, use the Airflow DAGs. See [Cluster Sizing](cluster-sizing.md#where-the-spark-driver-runs).

## Parallel Execution Demo

The `demo_all_pipelines_parallel` DAG submits the five local demos at the same time:

```
check_dependencies → check_keycloak → ┬─ run_noauth_pipeline
                                      ├─ run_basic_auth_pipeline
                                      ├─ run_bearer_pipeline
                                      ├─ run_oauth2_password_pipeline
                                      └─ run_oauth2_client_credentials_pipeline
```

Each job runs its driver on a worker and is capped at one executor core, so each job needs **2 task slots** (`WORKERS × WORKER_CORES` in total):

| Total slots | Behavior |
|---|---|
| 5 or fewer | **Hangs.** The five drivers take every slot, and no job can start an executor. |
| 6 (the default, 2 × 3) | Succeeds in about 2 minutes. Some jobs queue until others finish. |
| 10 or more | All five run at the same time. Needs about 5G of worker memory in total, for example `make up-demo WORKER_CORES=5 WORKER_MEMORY=3G`. |

See [Cluster Sizing](cluster-sizing.md) to choose values for your machine.

## Verifying Results

Each Airflow demo run writes 100 rows; each local-API notebook run writes 50. The tables are overwritten on each run.

### Via Spark SQL Shell

```bash
# Enter the Spark SQL shell:
make spark-sql
```

```sql
-- In the Spark SQL shell:
SHOW DATABASES;
USE demo;
SHOW TABLES;
SELECT status_code, count(*) FROM noauth_demo_response GROUP BY status_code;
SELECT * FROM noauth_demo_response LIMIT 10;
exit;
```

### Via PySpark Shell

```bash
# Enter the PySpark shell:
make spark-shell
```

```python
# List available tables (from Hive Metastore)
spark.sql("SHOW TABLES IN demo").show()

# Query a Delta table by name
spark.table("demo.noauth_demo_response").show()
```

An open `spark-sql` or `spark-shell` session holds free cores on the cluster until you exit it. Exit before triggering more DAGs, or they wait.

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

`minio-init` and `airflow-init` are missing from `make ps` once they finish. That's expected: they're one-shot setup containers.

### DAG fails with "connection refused"

- The DAGs need the mock API, which is only started with the `keycloak` profile. Start the platform with `make up-demo` (not `make up`).
- Check that it's running and healthy: `make ps`, or `curl http://localhost:8200/health`.

### OAuth2 demos fail to get token

- Keycloak takes up to a minute to start; wait until `make ps` shows it as `(healthy)`.
- Verify Keycloak is ready: `curl http://localhost:8180/health/ready`

### DAG stays in "running" forever

Open http://localhost:8080. If the applications show **WAITING** with 0 cores, the cluster doesn't have enough free task slots or memory:

- For `demo_all_pipelines_parallel`, you need `WORKERS × WORKER_CORES ≥ 6`.
- An open `make spark-sql` or `make spark-shell` session holds cores; exit it.
- See [Cluster Sizing](cluster-sizing.md#8-symptoms-and-fixes).

### Tables not found

- The Hive metastore must be healthy before running pipelines
- Check: `make logs SERVICE=hive-metastore`
- Databases are created on first pipeline run

### Re-run fails with `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`

A job was killed mid-write (for example, by resizing the cluster during a run), leaving files in MinIO without a table in the metastore. Run `make clean-volumes` (this deletes all demo tables and Airflow history) and start again with `make up-demo`.

### Checking Spark Driver Logs

When an Airflow demo fails, the Airflow task log shows the job's final state, and the Spark driver log shows the pipeline's own output and errors:

```bash
# Airflow task log
make dag-logs DAG=demo_noauth_pipeline TASK=run_noauth_pipeline

# Spark driver log of the most recent job (stdout = Python output, stderr = Spark/JVM log)
make driver-logs

# A specific driver; the ID (driver-YYYYMMDDhhmmss-NNNN) is in the Airflow task log
# and in the Spark master UI under "Completed Drivers"
make driver-logs ID=driver-20261106194805-0010
```

Driver logs live in the worker containers, so they're lost when the workers are recreated (for example, by resizing the cluster or `make down`).

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | Token: `jupyter` |
| Airflow | http://localhost:8088 | admin / admin |
| Spark Master UI | http://localhost:8080 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Keycloak Admin | http://localhost:8180 | admin / admin |
| Mock API Health | http://localhost:8200/health | -- |
