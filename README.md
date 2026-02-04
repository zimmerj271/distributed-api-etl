# Spark API‑Driven ETL Framework

A production‑grade, **configuration‑driven Spark ETL application** for ingesting data from files or tables, transforming it through Spark, and delivering records to external HTTP APIs at scale using Spark's parallelism. This application allows for configurable and dynamic input to provide a platform
for making API requests to any RESTfull API endpoint. The design is opinionated by defining a sink table which aligns to standards that allow 
request data to be loaded from any source API. The sink table defines the Bronze layer of a full ETL pipeline. Further ETL will be required to 
land the data into a silver table for downstream consumption. This application uses the following features:

* Spark-native distributed execution.
* Idempotent fault-tolerant API ingestion.
* Utilizes `aiohttp` to allow single threaded non-blocking asynchronous requests.
* Strict schema and config validation with `pydantic`.
* Explicit separation of *config*, *control*, and *runtime* layers.
* Explicit separation of *driver-side* and *worker-side* API -- full serialization of worker side logic.
* Extensible design for future scalability. Optional future extensions include:
  * Add `httpx` for HTTP requests.

---

## Table of Contents

1. [Who This Is For](#who-this-is-for)
2. [Key Features](#key-features)
3. [How the Pipeline Works](#how-the-pipeline-works)
4. [High‑Level Architecture](#high-level-architecture)
5. [Running the Pipeline](#running-the-pipeline)
6. [Configuration Overview](#configuration-overview)
7. [Full Configuration Template](#full-configuration-template)
8. [Design Principles](#design-principles)
9. [Middleware Model](#middleware-model)
10. [Transport](#transport)
11. [Local Development Platform](#local-development-platform)

---

## Who This Is For

This framework is intended for data engineers who need to:

* Land API request data directly into Delta tables.
* Send **millions of Spark rows to REST APIs** reliably
* Support **multiple auth strategies** (none, basic, bearer, OAuth2, mTLS)

It is especially well‑suited for:

* API‑based data enrichment as the bronze layer of a broader ETL pipeline.
* Regulatory or vendor API submissions (1‑row‑per‑request)
* Controlled rate‑limited ingestion pipelines

---

## Key Features

* **Spark‑native execution** to distribute requests across workers via `mapPartitions`
* **Factory‑based runtime objects** to allow serialization of worker-side logic (transport, auth, middleware)
* **Strict Pydantic config validation** (YAML / JSON)
* **Idempotent processing** via tracking requests with IDs and batch processing
* **Pluggable middleware pipeline** for request mutation, retries, logging, and enrichment
* **Reusable request templates** with row-level mutation via middleware
* **Worker-side execution model** with fully serialized runtime behavior
* **Databricks‑aware secrets resolution**

---

## How the Pipeline Works

At a high level:

1. A **pipeline config** is loaded and validated (YAML / JSON)
2. Configuration preprocessors resolve secrets and environment variables
3. Configuration is resolved through **Pydantic validation**
4. Runtime factories are created on the **driver**
5. Input data is read from:
   * A Spark table **or**
   * A provided DataFrame
4. The source data is repartitioned for parallelism
5. `mapPartitions` executes API calls **on worker nodes**
6. Results are collected and written to a sink table

Each Spark partition:

* Builds its own HTTP transport (`aiohttp` session + connector)
* Builds a reusable request template 
* Iterates row‑by‑row
* Materializes row-specific requests
* Sends exactly **one row per API request**

---

## High‑Level Architecture
### Driver-Side Architecture
![Driver Side](./docs/images/api_pipeline_driver.png)

### Worker-side Architecture
![Worker Side](./docs/images/api_pipeline_worker.png)


### Layer Responsibilities

| Layer   | Responsibility                    |
| ------- | --------------------------------- |
| Config  | Declarative pipeline definition   |
| Control | Validation, wiring, orchestration |
| Runtime | Executed on Spark workers         |

---

## Running the Pipeline

### Basic Usage

```python
from pipeline import run_pipeline

run_pipeline(
    spark=spark,
    config_path="configs/example_pipeline.yaml",
    source_df=None,  # or provide a DataFrame
)
```

### Source Resolution

* If `tables.source.identifier` is provided → read from Spark table
* If `source_df` is provided → use DataFrame directly
* Exactly **one** must be present

### Sink Behavior

* Results are written to the configured sink table
* Designed for downstream idempotent merges (SCD2‑safe)

---

## Configuration Overview

A pipeline configuration is composed of:

* **tables** – source & sink definitions
* **endpoint** – HTTP request definition
* **auth** – authentication strategy
* **transport** – HTTP engine selection
* **middleware** – retries, logging, auth injection
* **execution** – partitioning and rate control

All configs are **strictly validated** via Pydantic after preprocessing.

---

## Full Configuration Template

```yaml
execution:
  partitions: 200
  max_requests_per_second: 30

transport:
  base_timeout: 30
  warmup_timeout: 10
  tcp_connection:
    limit: 10

auth:
  type: oauth2_password
  token_url: "https://auth.example.com/token"
  client_id: "{{secret.DATABRICKS_SCOPE:CLIENT_ID_KEY}}"
  client_secret: "{{secret.DATABRICKS_SCOPE:CLIENT_SECRET_KEY}}"
  username: "{{secret.DATABRICKS_SCOPE:USERNAME_KEY}}"
  password: "{{secret.DATABRICKS_SCOPE:PASSWORD_KEY}}"

endpoint:
  method: POST
  base_path: "https://api.example.com/v1"
  url_path: "https://api.example.com/v1/patient"
  headers:
    Accept: "application/json"
    Content-Type: "application/json"

middleware:
  - type: retry
    max_attempts: 3
    retry_status_codes: [429, 500, 502, 503]
    base_delay: 0.2
    max_delay: 2.0
  - type: logging
  - type: timing
  - type: json_body

tables:
  source:
    name: api_staging
    namespace: catalog.schema
    id_column: tracking_id

    required_columns:
      - tracking_id
      - patient_id

  sink:
    name: api_response
    namespace: catalog.schema
    mode: append

  column_mapping:
    - source_column: patient_id
      endpoint_param: patient

```

### Secrets Handling

* Secrets are resolved **before** Pydantic validation
* Databricks secrets are accessed via `dbutils.secrets`

Runtime components never access environment variables or secret stores directly.

---

## Design Principles

### 1. Spark Safety

* No SparkContext on workers
* Only serializable factories are shipped

### 2. Idempotency First

* Every row carries a `request_id`
* Designed for merge-friendly downstream tables

### 3. Compile, Then Run

* Config is preprocessed, validated, and *compiled* on the driver
* Workers execute only runtime logic

### 3. Clear Abstractions

* Config ≠ Control ≠ Runtime
* Each layer has a single responsibility

### 4. Minimal Magic

* Explicit wiring
* No hidden global state
* No runtime config mutation

---

## Middleware Model

This framework uses a **middleware pipeline** to customize request behavior without modifying
core execution logic. Middleware is executed **on Spark worker nodes** and wraps each API request
using a chain-of-responsibility pattern.

Middleware allows users to:

* Inject authentication or headers
* Retry failed requests with backoff
* Record timing and diagnostics
* Enrich or inspect request / response metadata
* Mutate outbound requests on a per-row basis

### Middleware Execution Order

Middleware is executed **in the order it is configured**.

Each middleware wraps the next:
Middleware A
→ Middleware B
→ Middleware C
→ HTTP Request
← Middleware C
← Middleware B
← Middleware A

This allows middleware to run logic **before and/or after** the HTTP request.

---

### Middleware Categories

Middleware is intentionally divided into **three conceptual categories**:

#### 1. Interceptors (Control-Flow Middleware)

Interceptors **modify request or response behavior** and may influence execution flow.

Examples:
* Retry logic
* Authentication injection
* Query parameter injection
* JSON parsing
* Request/response mutation

These middleware **must run before the HTTP request**.

#### 2. Listeners (Observability Middleware)

Listeners **observe and record metadata** but do not alter request/response semantics.

Examples:
* Logging
* Timing
* Worker / executor identity
* Diagnostics

Listeners are safe to run anywhere in the pipeline and never affect request success or retries.

#### 3. Common (Mutate Requests Only)

Standard middleware that does not follow the interceptor pattern. These middleware objects only mutate requests.


---

### Adding Custom Middleware
To add custom middleware, implement the following interface:

```python
async def __call__(
  self,
  request_exchange: RequestExchange,
  next_call: NEXT_CALL
) -> RequestExchange: ...
```

Then register it via configuration, factory injection or late binding in `ApiPartitionExecutor`.

---

### Middleware Configuration

Middleware is configured declaratively and executed in the order listed.

Each middleware entry defines:
* The middleware type
* Optional configuration parameters

Example:

```yaml
middleware:
  - type: retry
    max_attempts: 3
    retry_status_codes: [429, 500, 502, 503]
    base_delay: 0.2
    max_delay: 2.0
  - type: logging
  - type: timing
  - type: json_body
```

Middleware types are resolved to runtime implementations on the worker.

Custom middleware can be added by extending the middleware registry and providing
a factory function that returns a callable middleware object.

---

### 5. Middleware-Driven Extensibility

* Request behavior is modified via middleware, not hard-coded logic
* Middleware is composable, reusable, and ordered
* New functionality is added without changing the executor or transport layers

## Common Middleware Included

| Middleware | Category | Purpose |
|----------|----------|---------|
| RetryMiddleware | Interceptor | Retry failed requests with exponential backoff |
| JsonResponseMiddleware | Interceptor | Parse and validate JSON responses |
| AuthMiddleware | Interceptor | Inject authentication headers |
| ParamInjectorMiddleware | Interceptor | Inject row-level query parameters |
| LoggingMiddleware | Listener | Record request/response logs |
| TimingMiddleware | Listener | Measure request latency |
| WorkerIdentityMiddleware | Listener | Record executor identity |
| TransportDiagnosticMiddleware | Listener | Record transport diagnostics |
| BearerTokenMiddleware | Common | Injects bearer token into request header |
| HeaderAuthMiddleware | Common | Injects user authentication into header |

---

## Transport

### Purpose of the Transport Layer

The **transport layer** is responsible for executing HTTP requests on Spark **worker nodes**.
It is intentionally designed as a **low-level**, **runtime-only abstraction** that focuses solely on
*network execution* and *connection management*, not request semantics.

The transport layer is designed to be:
* **Spark-safe** 
  (fully serializable, no driver-side state)
* **Process-scoped (executor-local)** 
  One transport instance per Python worker process, reused across multiple partitions
* **Efficient and stable** 
  Connection pooling, warm-up, reuse across partitions to avoid cold-start effects.
* **Extensible** 
  Multiple HTTP engines can be added and supported via factories

All higher-level concerns—authentication, retries, logging, request mutation, response handling—are
**explicitly excluded** from the transport layer and are handled by middleware.

### Transport Execution Model

Spark executes user code in long-lived **Python worker processes** on executor nodes.
The transport layer is designed around this execution model.

Each Python worker process follows this lifecycle:

1) Spark deserializes the partition execution function into the worker process
2) A `worker-local resource manager` is lazily initialized 
3) A single transport instance is created **once per worker process**
4) The transport is reused across: 
  * multiple rows
  * multiple partitions
5) When the worker process exits, asynchronous resources are shut down gracefully

This design intentionally avoids:
* Creating a new HTTP session per row
* Creating a new HTTP session per partition
* Repeated DNS lookups, TCP handshakes, and TLS negotiations.
* Spark serialization failures caused by leaking runtime state

  **Important:**
  Transport reuse is scoped to the Python worker process-not the driver or the cluster.
  Each executor maintains its own independent transport instance.


### Transport vs Middleware Responsibilities

The transport layer is intentionally minimal.

| Concern | Transport| Middleware |
|---------|----------|------------|
|Connection pooling|✅|❌|
|TCP / TLS / DNS|✅|❌
|Warm-up|✅|❌|
|Authentication|❌|✅
|Retries / backoff|❌|✅
|Logging / metrics|❌|✅
|Request mutation|❌|✅
|Response parsing|❌|✅

This separation ensures that:
* transports remain interchangeable
* request behavior is fully configurable
* adding new transports does not affect pipeline logic

### Supported Transport Engines

| Engine | Status | Notes |
|--------|--------|-------|
| `aiohttp` | Default | Async, pooled, warm-up enabled |
| `httpx` | Planned | Async + sync supported |

---

## Local Development Platform

This project includes a fully containerized local development environment managed by Docker Compose. The platform replicates a production-like data engineering stack, allowing you to develop, test, and debug ETL pipelines locally before deploying to a cloud environment such as Databricks.

All services are defined in `docker/docker-compose.yml` and orchestrated through the `Makefile`.

### Prerequisites

- Docker and Docker Compose V2
- `direnv` (optional, for automatic environment variable loading from `.envrc`)
- GNU Make

### Platform Architecture

The platform is composed of four layers: **infrastructure**, **catalog**, **compute**, and **orchestration**, with optional services for development and diagnostics.

```
                         +-------------------+
                         |  Airflow Web UI   |  :8088
                         +-------------------+
                                  |
                         +-------------------+
                         | Airflow Scheduler |
                         +--------+----------+
                                  | submits jobs
                         +--------v----------+
                         |   Spark Master    |  :7077 / :8080
                         +--------+----------+
                              /        \
                    +--------v--+  +--v--------+
                    |  Worker 1 |  |  Worker 2 |
                    +-----------+  +-----------+
                         |              |
              reads/writes data    queries schema
                    |                   |
             +------v------+    +------v--------+
             |    MinIO     |   | Hive Metastore |  :9083
             | (S3 storage) |   +-------+--------+
             |  :9000/:9001 |           |
             +------+-------+    +------v------+
                    |            |  PostgreSQL  |  :5432
                    +----------->|  (metadata)  |
                                 +-------------+
```

### Service Reference

#### Infrastructure Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `postgres` | `postgres:15-alpine` | `postgres` | 5432 | Shared relational database for Hive Metastore catalog and Airflow metadata |
| `minio` | `minio/minio:latest` | `minio` | 9000 (API), 9001 (Console) | S3-compatible object store serving as the data lake for Parquet and Delta Lake files |
| `minio-init` | `minio/mc:latest` | `minio-init` | -- | One-shot container that creates `warehouse` and `spark-logs` buckets, then exits |

**PostgreSQL** hosts two databases, created by `docker/postgres/init-multiple-dbs.sh`:
- `metastore` -- Hive Metastore schema catalog
- `airflow` -- Airflow task history, DAG state, and user accounts

PostgreSQL is not an HTTP service. Connect via a database client (`psql`, DBeaver, etc.) or through Docker:
```bash
docker exec -it postgres psql -U postgres
```

**MinIO** provides two interfaces: the S3 API on port 9000 (used programmatically by Spark, Hive, and Airflow) and a web console on port 9001 for browsing buckets and objects manually at `http://localhost:9001`.

#### Catalog Service

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `hive-metastore` | `distributed-api-etl-hive:latest` | `hive-metastore` | 9083 | Schema catalog for the data lake -- stores table names, column definitions, partition layouts, and S3 paths |

**Hive Metastore** does not process data. It is a metadata service that Spark queries over Thrift (port 9083) to resolve table schemas and data file locations. When Spark executes `spark.sql("SELECT * FROM my_table")`, the metastore tells Spark which columns exist and where the underlying Parquet/Delta files reside in MinIO. The metastore persists its catalog in the `metastore` PostgreSQL database.

This service uses a custom Docker image (`docker/hive/Dockerfile`) that extends `apache/hive:4.0.0` with the PostgreSQL JDBC driver and baked-in configuration files (`hive-site.xml`, `core-site.xml`).

#### Compute Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `spark-master` | `distributed-api-etl-spark:latest` | `spark-master` | 7077 (cluster), 8080 (UI) | Spark cluster manager -- coordinates resource allocation and job distribution |
| `spark-worker` | `distributed-api-etl-spark:latest` | auto-generated | -- | Spark execution nodes (2 replicas by default) -- run partition-level tasks |

**Spark Master** accepts job submissions on port 7077 and distributes work across workers. The web UI at `http://localhost:8080` shows active applications, worker status, and running jobs.

**Spark Workers** register with the master and provide CPU and memory for task execution. Each worker reads data from MinIO, processes it, and writes results back. The number of workers is controlled by the `SPARK_WORKERS` environment variable (default: 2) or the `WORKERS` Makefile variable.

Both use a custom image (`docker/spark/Dockerfile`) based on `apache/spark:3.5.1` with Delta Lake, Hadoop AWS, and project Python dependencies pre-installed. Source code (`src/`, `tests/`, `dags/`) is mounted read-only so workers have access to application code without rebuilding the image.

#### Orchestration Services

| Service | Image | Container | Ports | Purpose |
|---------|-------|-----------|-------|---------|
| `airflow-init` | `distributed-api-etl-airflow:latest` | `airflow-init` | -- | One-shot container that runs database migrations and creates the admin user, then exits |
| `airflow-webserver` | `distributed-api-etl-airflow:latest` | `airflow-webserver` | 8088 | Web UI for viewing, triggering, and monitoring DAGs |
| `airflow-scheduler` | `distributed-api-etl-airflow:latest` | `airflow-scheduler` | -- | Execution engine that monitors DAG definitions and runs tasks on schedule |

**Airflow** orchestrates pipeline execution. The scheduler reads DAG definitions from the `dags/` directory, determines which tasks are ready based on schedules and dependencies, and executes them using `LocalExecutor` (tasks run as subprocesses within the scheduler container). When a DAG task needs to run a Spark job, it submits it to the spark-master.

The `airflow-init` container is a synchronization point: it migrates the database and creates the admin user exactly once before the webserver and scheduler start. Both long-running services declare a dependency on `airflow-init` completing successfully, preventing race conditions during database setup.

Access the Airflow UI at `http://localhost:8088`.

#### Optional Services (Profile-gated)

| Service | Image | Container | Ports | Profile | Purpose |
|---------|-------|-----------|-------|---------|---------|
| `spark-history-server` | `distributed-api-etl-spark:latest` | `spark-history-server` | 18080 | `history` | Read-only web UI for inspecting completed Spark jobs |
| `jupyter` | `distributed-api-etl-jupyter:latest` | `jupyter` | 8888 | `jupyter` | JupyterLab for interactive development and data exploration |

These services are excluded from `docker compose up` by default. They start only when their profile is activated (see [Makefile Commands](#makefile-commands) below).

**Spark History Server** reads event log files from the `spark-logs` volume and reconstructs the Spark monitoring UI for completed applications. It is useful for post-hoc debugging -- for example, investigating why an overnight Spark job ran slowly. The event logs are written by Spark regardless of whether the History Server is running, so it can be started on demand.

**Jupyter** provides a JupyterLab notebook server connected to the Spark cluster. It mounts `src/` read-only and `notebooks/` read-write, allowing interactive prototyping of ETL logic, ad-hoc data exploration, and queries against the Hive Metastore before codifying work into Airflow DAGs.

### Data Flow

1. **Airflow scheduler** triggers a DAG task on a schedule or manual trigger
2. The task **submits a Spark job** to the spark-master
3. Spark-master distributes work across **spark-workers**
4. Workers query **hive-metastore** to resolve table schemas and data file locations
5. Workers read source data from and write results to **MinIO**
6. Hive-metastore records new or updated table metadata in **PostgreSQL**
7. Airflow logs task outcomes to **PostgreSQL**

### Startup Order

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

### Network and Volumes

All services communicate over a single Docker bridge network (`etl-network`), which provides DNS resolution by container name (e.g., `spark-master`, `postgres`, `minio`).

| Volume | Purpose |
|--------|---------|
| `postgres-data` | PostgreSQL data directory (Hive catalog + Airflow metadata) |
| `minio-data` | MinIO object storage (data lake files) |
| `hive-warehouse` | Hive warehouse directory |
| `spark-logs` | Spark event logs (consumed by History Server) |
| `airflow-logs` | Airflow task execution logs |

Volumes persist data across container restarts. Use `make clean-volumes` to delete all volumes and reinitialize from scratch.

### Port Reference

| Port | Service | Protocol | URL |
|------|---------|----------|-----|
| 5432 | PostgreSQL | PostgreSQL wire protocol | `psql -h localhost -U postgres` |
| 7077 | Spark Master | Spark RPC | `spark://localhost:7077` |
| 8080 | Spark Master UI | HTTP | `http://localhost:8080` |
| 8088 | Airflow Web UI | HTTP | `http://localhost:8088` |
| 8888 | Jupyter | HTTP | `http://localhost:8888` |
| 9000 | MinIO S3 API | HTTP | `http://localhost:9000` |
| 9001 | MinIO Console | HTTP | `http://localhost:9001` |
| 9083 | Hive Metastore | Thrift | `thrift://localhost:9083` |
| 18080 | Spark History Server | HTTP | `http://localhost:18080` |

### Default Credentials

Credentials are defined in `.envrc` and passed to containers via environment variable substitution in `docker-compose.yml`. Run `direnv allow` to load them automatically, or export them manually before starting the stack.

| Service | Username | Password |
|---------|----------|----------|
| MinIO | `minioadmin` | `minioadmin` |
| Airflow | `admin` | `admin` |
| PostgreSQL | `postgres` | `postgres` |
| Jupyter | -- | Token: `jupyter` |

### Custom Docker Images

Three services require custom images that must be built before starting the stack:

| Image | Dockerfile | Base | Additions |
|-------|-----------|------|-----------|
| `distributed-api-etl-spark` | `docker/spark/Dockerfile` | `apache/spark:3.5.1` | Delta Lake JARs, Hadoop AWS SDK, Python dependencies |
| `distributed-api-etl-airflow` | `docker/airflow/Dockerfile` | `apache/airflow:2.8.1` | Java 17 (for Spark submit), Spark and AWS Airflow providers |
| `distributed-api-etl-hive` | `docker/hive/Dockerfile` | `apache/hive:4.0.0` | PostgreSQL JDBC driver, Hive and S3 configuration |
| `distributed-api-etl-jupyter` | `docker/jupyter/Dockerfile` | -- | JupyterLab with PySpark and project dependencies (optional) |

Build all required images with `make build`. Build the optional Jupyter image with `make build-jupyter`.

### Makefile Commands

| Command | Description |
|---------|-------------|
| `make up` | Start all default services |
| `make up-jupyter` | Start all services including Jupyter |
| `make up-history` | Start all services including Spark History Server |
| `make up-all` | Start all services including all optional profiles |
| `make down` | Stop all services (including optional profiles) |
| `make restart` | Stop and start all default services |
| `make logs` | Tail logs for all services |
| `make logs SERVICE=<name>` | Tail logs for a specific service |
| `make ps` | Show running containers |
| `make build` | Build all required custom images (Spark, Airflow, Hive) |
| `make build-spark` | Build the Spark image |
| `make build-airflow` | Build the Airflow image |
| `make build-hive` | Build the Hive Metastore image |
| `make build-jupyter` | Build the Jupyter image (optional) |
| `make clean` | Stop containers and remove networks and orphans |
| `make clean-volumes` | Stop containers and remove all volumes (deletes data) |
| `make clean-all` | Full cleanup including volumes and locally built images |
| `make spark-shell` | Open an interactive PySpark shell on the cluster |
| `make spark-submit APP=<path>` | Submit a Spark job to the cluster |
| `make spark-sql` | Open an interactive Spark SQL shell |
| `make shell CONTAINER=<name>` | Open a bash shell in a running container |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests only |
| `make lint` | Run linter (`ruff check`) |
| `make format` | Format code (`ruff format`) |
| `make typecheck` | Run type checker (`pyright`) |

### Quick Start

```bash
# 1. Allow direnv to load environment variables
direnv allow

# 2. Build all required images
make build

# 3. Start the platform
make up

# 4. Verify all services are running
make ps

# 5. Access the UIs
#    Spark Master:  http://localhost:8080
#    Airflow:       http://localhost:8088
#    MinIO Console: http://localhost:9001
```
