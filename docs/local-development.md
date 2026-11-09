# Local Development Platform

This project includes a fully containerized local development environment managed by Docker Compose. The platform replicates a production-like data engineering stack, allowing you to develop, test, and debug ETL pipelines locally before deploying to a cloud environment such as Databricks.

All services are defined in `docker/docker-compose.yml` and orchestrated through the `Makefile`.

## Prerequisites

See [Requirements](../README.md#requirements) in the README for the required tools, hardware, and ports.

All platform settings are passed as Makefile parameters (for example `make up-demo WORKERS=3`). The
project doesn't use `.env` files. The service credentials are fixed demo defaults (see
[Default Credentials](#default-credentials)); `docker/spark/spark-defaults.conf` and
`docker/jupyter/spark-defaults.conf` hardcode the MinIO credentials, so don't change them in only
one place.

## Platform Architecture

The platform is composed of four layers: **infrastructure**, **catalog**, **compute**, and **orchestration**, with optional services for development and diagnostics.

```mermaid
flowchart TB
    airflow_ui["Airflow Web UI<br>─────────────<br>:8088"]
    scheduler["Airflow Scheduler"]
    master["Spark Master<br>─────────────<br>:7077 / :8080 / :6066"]

    subgraph workers["Spark Workers"]
        direction LR
        w1["Worker 1"]
        w2["Worker 2"]
    end

    minio[("MinIO (S3)<br>─────────────<br>:9000 / :9001")]
    hive["Hive Metastore<br>─────────────<br>:9083"]
    postgres[("PostgreSQL<br>─────────────<br>:5432")]

    subgraph optional["Optional Services (--profile keycloak)"]
        direction LR
        keycloak["Keycloak<br>─────────────<br>:8180"]
        mock_api["Mock API<br>─────────────<br>:8200"]
    end

    airflow_ui --> scheduler
    scheduler -->|"submits jobs via REST API"| master
    master --> workers
    w1 & w2 -->|"reads/writes data"| minio
    w1 & w2 -->|"queries schema"| hive
    w1 & w2 -->|"HTTP requests"| mock_api
    minio -->|"metadata"| postgres
    hive --> postgres
    keycloak -->|"issues tokens"| w1 & w2

    style airflow_ui fill:#1168bd,stroke:#0d4884,color:#fff
    style scheduler fill:#1168bd,stroke:#0d4884,color:#fff
    style master fill:#e67e22,stroke:#d35400,color:#fff
    style workers fill:#1a1a2e,stroke:#4a4a8a,color:#fff
    style w1 fill:#17a2b8,stroke:#117a8b,color:#fff
    style w2 fill:#17a2b8,stroke:#117a8b,color:#fff
    style minio fill:#28a745,stroke:#1e7e34,color:#fff
    style hive fill:#6c757d,stroke:#495057,color:#fff
    style postgres fill:#6c757d,stroke:#495057,color:#fff
    style optional fill:#1a1a2e,stroke:#4a4a8a,color:#fff,stroke-dasharray: 5 5
    style keycloak fill:#dc3545,stroke:#bd2130,color:#fff
    style mock_api fill:#dc3545,stroke:#bd2130,color:#fff
```


## Service Reference

### Infrastructure Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `postgres` | `postgres:15-alpine` | `postgres` | 5432 | Shared relational database for Hive Metastore catalog and Airflow metadata |
| `minio` | `ghcr.io/zimmerj271/silo` (pinned release) | `minio` | 9000 (API), 9001 (Console) | S3-compatible object store serving as the data lake for Parquet and Delta Lake files |
| `minio-init` | `ghcr.io/zimmerj271/mc` (pinned release) | `minio-init` | -- | One-shot container that creates `warehouse` and `spark-logs` buckets, then exits |

The upstream MinIO images were discontinued, so the stack uses Silo and `mc` images from a mirrored
repository instead, pinned by digest in `docker/docker-compose.yml`. The service is still called
`minio` and is used in the same way.

**PostgreSQL** hosts two databases, created by `docker/postgres/init-multiple-dbs.sh`:
- `metastore` -- Hive Metastore schema catalog
- `airflow` -- Airflow task history, DAG state, and user accounts

Connect via Docker:
```bash
docker exec -it postgres psql -U postgres
```

**MinIO** provides two interfaces: the S3 API on port 9000 (used programmatically by Spark, Hive, and Airflow) and a web console on port 9001 for browsing buckets and objects manually at `http://localhost:9001`.

### Catalog Service

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `hive-metastore` | `distributed-api-etl-hive:latest` | `hive-metastore` | 9083 | Schema catalog for the data lake |

**Hive Metastore** does not process data. It is a metadata service that Spark queries over Thrift (port 9083) to resolve table schemas and data file locations. The metastore persists its catalog in the `metastore` PostgreSQL database.

### Compute Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `spark-master` | `distributed-api-etl-spark:latest` | `spark-master` | 7077 (cluster), 8080 (UI), 6066 (REST API) | Spark cluster manager |
| `spark-worker` | `distributed-api-etl-spark:latest` | `distributed-api-etl-spark-worker-N` | -- | Spark execution nodes (2 by default) |

**Spark Master** accepts job submissions on port 7077 (RPC) or 6066 (REST API) and distributes work across workers. The web UI at `http://localhost:8080` shows active applications and worker status.

**Spark Workers** register with the master and provide task slots and memory for task execution. The cluster is sized with three Makefile parameters, passed to any `make up-*` target:

| Parameter | Default | Meaning |
|---|---|---|
| `WORKERS` | `2` | Number of worker containers |
| `WORKER_CORES` | `3` | Task slots per worker |
| `WORKER_MEMORY` | `2G` | Memory per worker for Spark drivers and executors |

For example: `make up-demo WORKERS=3 WORKER_CORES=2 WORKER_MEMORY=3G`. See [Cluster Sizing](cluster-sizing.md) for how to choose values, or run `make sizing`.

How they reach Docker Compose: `WORKERS` becomes `--scale spark-worker=N` on the `docker compose up` command. `WORKER_CORES` and `WORKER_MEMORY` are exported as `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY`, which `docker-compose.yml` passes to the worker containers. The fallback values in `docker-compose.yml` (`deploy.replicas: 2`, `:-3`, `:-2G`) only apply to plain `docker compose` commands run without the Makefile.

### Orchestration Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `airflow-init` | `distributed-api-etl-airflow:latest` | `airflow-init` | -- | One-shot: runs migrations and creates admin user |
| `airflow-webserver` | `distributed-api-etl-airflow:latest` | `airflow-webserver` | 8088 | Web UI for viewing and triggering DAGs |
| `airflow-scheduler` | `distributed-api-etl-airflow:latest` | `airflow-scheduler` | -- | Execution engine that monitors and runs tasks |

Access the Airflow UI at `http://localhost:8088`.

### Optional Services (Profile-gated)

| Service | Image | Ports | Profile | Purpose |
|---------|-------|-------|---------|---------|
| `spark-history-server` | `distributed-api-etl-spark:latest` | 18080 | `history` | Read-only UI for inspecting completed Spark jobs |
| `jupyter` | `distributed-api-etl-jupyter:latest` | 8888 | `jupyter` | JupyterLab for interactive development |
| `keycloak` | `quay.io/keycloak/keycloak:24.0` | 8180 | `keycloak` | OAuth2/OIDC identity provider |
| `mock-api` | `distributed-api-etl-mock-api:latest` | 8200 | `keycloak` | FastAPI mock server for testing auth patterns |

These services start only when their profile is activated.

## Data Flow

1. **Airflow scheduler** triggers a DAG task
2. The task **submits a Spark job** to the spark-master via REST API
3. Spark-master distributes work across **spark-workers**
4. Workers query **hive-metastore** to resolve table schemas
5. Workers read source data from and write results to **MinIO**
6. Hive-metastore records table metadata in **PostgreSQL**

## Startup Order

Docker Compose enforces the following dependency chain via `depends_on` with health checks:

```
postgres (healthy)
  ├── minio-init (completed) ← minio (healthy)
  │     └── hive-metastore (healthy)
  │           └── spark-master (healthy)
  │                 ├── spark-worker (×N)
  │                 ├── spark-history-server (optional)
  │                 └── jupyter (optional)
  └── airflow-init (completed)
        ├── airflow-webserver
        └── airflow-scheduler
```

## Network and Volumes

All services communicate over a single Docker bridge network (`etl-network`), which provides DNS resolution by container name.

| Volume | Purpose |
|--------|---------|
| `postgres-data` | PostgreSQL data directory |
| `minio-data` | MinIO object storage |
| `hive-warehouse` | Hive warehouse directory |
| `spark-logs` | Spark event logs |
| `airflow-logs` | Airflow task logs |

Volumes persist data across container restarts. Use `make clean-volumes` to delete all volumes.

## Port Reference

| Port | Service | URL |
|------|---------|-----|
| 5432 | PostgreSQL | `psql -h localhost -U postgres` |
| 6066 | Spark REST API | `http://localhost:6066` |
| 7077 | Spark Master | `spark://localhost:7077` |
| 8080 | Spark Master UI | `http://localhost:8080` |
| 8088 | Airflow Web UI | `http://localhost:8088` |
| 8180 | Keycloak Admin | `http://localhost:8180` |
| 8200 | Mock API | `http://localhost:8200` |
| 8888 | Jupyter | `http://localhost:8888` |
| 9000 | MinIO S3 API | `http://localhost:9000` |
| 9001 | MinIO Console | `http://localhost:9001` |
| 9083 | Hive Metastore | `thrift://localhost:9083` |
| 18080 | Spark History Server | `http://localhost:18080` |

## Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| MinIO | `minioadmin` | `minioadmin` |
| Airflow | `admin` | `admin` |
| PostgreSQL | `postgres` | `postgres` |
| Jupyter | -- | Token: `jupyter` |
| Keycloak Admin | `admin` | `admin` |

## Custom Docker Images

| Image | Dockerfile | Base | Additions |
|-------|-----------|------|-----------|
| `distributed-api-etl-spark` | `docker/spark/Dockerfile` | `apache/spark:3.5.1` | Delta Lake, Hadoop AWS, Python deps |
| `distributed-api-etl-airflow` | `docker/airflow/Dockerfile` | `apache/airflow:2.8.1` | HTTP providers, requests |
| `distributed-api-etl-hive` | `docker/hive/Dockerfile` | `apache/hive:4.0.0` | PostgreSQL JDBC driver |
| `distributed-api-etl-jupyter` | `docker/jupyter/Dockerfile` | `jupyter/pyspark-notebook` | PySpark, project deps |
| `distributed-api-etl-mock-api` | `docker/mock-api/Dockerfile` | `python:3.10-alpine` | FastAPI mock server |

Build all required images with `make build`.

## Makefile Commands

### Starting Services

| Command | Description |
|---------|-------------|
| `make up` | Start all default services |
| `make up-jupyter` | Start with Jupyter |
| `make up-history` | Start with Spark History Server |
| `make up-keycloak` | Start with Keycloak and mock-api |
| `make up-demo` | Start all services for demos |
| `make up-all` | Start all services including all profiles |
| `make down` | Stop and remove all containers (data volumes are kept) |
| `make restart` | Restart running containers in place (keeps profiles and cluster size; doesn't apply changes) |

All `up` targets accept `WORKERS`, `WORKER_CORES`, and `WORKER_MEMORY`. Run `make sizing` to get suggested values for your machine.

### Building Images

| Command | Description |
|---------|-------------|
| `make build` | Build all required images |
| `make build-demo` | Build all images for demos |
| `make build-spark` | Build Spark image |
| `make build-airflow` | Build Airflow image |
| `make build-hive` | Build Hive Metastore image |
| `make build-jupyter` | Build Jupyter image |
| `make build-mock-api` | Build mock API image |

### Operations

| Command | Description |
|---------|-------------|
| `make logs` | Tail logs for all services |
| `make logs SERVICE=<name>` | Tail logs for a specific service |
| `make ps` | Show running containers |
| `make shell CONTAINER=<name>` | Open bash shell in a service (use the service name, e.g. `spark-master`) |
| `make sizing` | Suggest cluster parameters for this machine |
| `make clean` | Stop containers and remove networks |
| `make clean-volumes` | Stop and remove all volumes |
| `make clean-all` | Full cleanup including images |

### Spark Operations

| Command | Description |
|---------|-------------|
| `make spark-shell` | Open PySpark shell |
| `make spark-submit APP=<path>` | Submit Spark job |
| `make spark-sql` | Open Spark SQL shell |
| `make driver-logs [ID=driver-...]` | Show a Spark driver's logs from the worker it ran on (default: latest) |

### Airflow Operations

| Command | Description |
|---------|-------------|
| `make dag-list` | List DAGs and whether they're paused |
| `make dag-trigger DAG=<dag_id>` | Unpause and trigger a DAG |
| `make dag-status` | Latest run of every DAG |
| `make dag-status DAG=<dag_id> [RUN=<run_id>]` | Recent runs of one DAG; with `RUN`, the state of each task |
| `make dag-logs DAG=<dag_id> TASK=<task_id> [RUN=<run_id>]` | Latest Airflow log for a task |

### Development

These run on the host and need the local Python environment (see [Local Development](../README.md#local-development) in the README).

| Command | Description |
|---------|-------------|
| `make install` | Create or update `.venv` with uv |
| `make requirements` | Regenerate `requirements.txt` from `uv.lock` |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests |
| `make test-integration` | Run integration tests |
| `make lint` | Run linter |
| `make format` | Format code |
| `make typecheck` | Run type checker |
