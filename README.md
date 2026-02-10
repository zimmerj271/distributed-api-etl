# Spark API-Driven ETL Framework

A production-grade, **configuration-driven Spark ETL application** for ingesting data from files or tables, transforming it through Spark, and delivering records to external HTTP APIs at scale using Spark's parallelism.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SPARK CLUSTER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐         ┌─────────────────────────────────────────────┐  │
│  │    Driver    │         │              Spark Workers                  │  │
│  │              │         │  ┌─────────┐  ┌─────────┐  ┌─────────┐     │  │
│  │  • Load YAML │ ──────▶ │  │Partition│  │Partition│  │Partition│     │  │
│  │  • Validate  │         │  │    1    │  │    2    │  │    N    │     │  │
│  │  • Partition │         │  │         │  │         │  │         │     │  │
│  │              │         │  │ HTTP ──▶│  │ HTTP ──▶│  │ HTTP ──▶│     │  │
│  └──────────────┘         │  │ Request │  │ Request │  │ Request │     │  │
│         │                 │  └────┬────┘  └────┬────┘  └────┬────┘     │  │
│         │                 └───────┼────────────┼────────────┼──────────┘  │
│         │                         │            │            │             │
│         ▼                         ▼            ▼            ▼             │
│  ┌──────────────┐         ┌─────────────────────────────────────────────┐  │
│  │  Sink Table  │ ◀────── │            API Responses                    │  │
│  │ (Delta Lake) │         └─────────────────────────────────────────────┘  │
│  └──────────────┘                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

* **Spark-native execution** - Distributes API requests across workers via `mapPartitions`
* **Configuration-driven** - Define pipelines in YAML with strict Pydantic validation
* **Multiple auth patterns** - None, Basic, Bearer, OAuth2 (password & client credentials)
* **Pluggable middleware** - Retries, logging, timing, auth injection, request mutation
* **Idempotent processing** - Track requests with IDs for fault-tolerant execution
* **Worker-side execution** - Fully serialized runtime behavior for Spark safety

## Quick Start

```bash
# 1. Load environment variables
direnv allow

# 2. Build Docker images
make build-demo

# 3. Start the platform
make up-demo

# 4. Wait for services (~60-90s for all health checks)
make ps

# 5. Access the UIs
#    Jupyter:      http://localhost:8888  (token: jupyter)
#    Airflow:      http://localhost:8088  (admin/admin)
#    Spark Master: http://localhost:8080
#    MinIO:        http://localhost:9001  (minioadmin/minioadmin)
```

## Running Pipelines

### Via Jupyter Notebooks

1. Open http://localhost:8888 (token: `jupyter`)
2. Navigate to `notebooks/` and open any demo notebook
3. Run all cells to execute the pipeline

### Via Airflow UI

1. Open http://localhost:8088 (admin/admin)
2. Find `demo_all_pipelines_parallel` (runs all demos in parallel)
3. Toggle the DAG to "On" and click the play button

### Via Airflow CLI

```bash
# Unpause and trigger the parallel demo
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags unpause demo_all_pipelines_parallel

docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags trigger demo_all_pipelines_parallel

# Monitor status
docker compose -f docker/docker-compose.yml exec airflow-webserver \
  airflow dags list-runs -d demo_all_pipelines_parallel
```

### Programmatic Usage

```python
from pipeline import run_pipeline

run_pipeline(
    spark=spark,
    config_path="configs/example_pipeline.yaml",
    source_df=None,  # or provide a DataFrame
)
```

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | High-level design, data flow, and design principles |
| [Configuration](docs/configuration.md) | Pipeline YAML configuration reference |
| [Middleware](docs/middleware.md) | Middleware pipeline and customization |
| [Transport](docs/transport.md) | HTTP transport layer and connection pooling |
| [Local Development](docs/local-development.md) | Docker Compose platform and Makefile commands |
| [Running Demos](docs/running-demos.md) | Step-by-step guide to running demo pipelines |
| [Spark REST API](docs/spark-rest-api-configuration.md) | How Airflow submits jobs via Spark REST API |

## Platform Architecture

```
                         ┌───────────────────┐
                         │  Airflow Web UI   │  :8088
                         └─────────┬─────────┘
                                   │
                         ┌─────────▼─────────┐
                         │ Airflow Scheduler │
                         └─────────┬─────────┘
                                   │ submits jobs via REST API
                         ┌─────────▼─────────┐
                         │   Spark Master    │  :7077 / :8080 / :6066
                         └─────────┬─────────┘
                              ┌────┴────┐
                         ┌────▼───┐ ┌───▼────┐
                         │Worker 1│ │Worker 2│
                         └────┬───┘ └───┬────┘
                              │         │
               reads/writes data    queries schema
                         │              │
                    ┌────▼────┐    ┌────▼─────────┐
                    │  MinIO  │    │Hive Metastore│  :9083
                    │   (S3)  │    └──────┬───────┘
                    │:9000/01 │           │
                    └────┬────┘    ┌──────▼──────┐
                         │         │ PostgreSQL  │  :5432
                         └────────▶│ (metadata)  │
                                   └─────────────┘
```

## Common Commands

| Command | Description |
|---------|-------------|
| `make up` | Start core services |
| `make up-demo` | Start all services for demos |
| `make down` | Stop all services |
| `make logs SERVICE=<name>` | View logs for a service |
| `make ps` | Show running containers |
| `make spark-sql` | Open Spark SQL shell |
| `make test` | Run all tests |

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter | http://localhost:8888 | Token: `jupyter` |
| Airflow | http://localhost:8088 | admin / admin |
| Spark Master | http://localhost:8080 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Keycloak | http://localhost:8180 | admin / admin |

## Who This Is For

This framework is intended for data engineers who need to:

* Land API request data directly into Delta tables
* Send **millions of Spark rows to REST APIs** reliably
* Support **multiple auth strategies** (none, basic, bearer, OAuth2)

It is especially well-suited for:

* API-based data enrichment as the bronze layer of a broader ETL pipeline
* Regulatory or vendor API submissions (1-row-per-request)
* Controlled rate-limited ingestion pipelines

## License

MIT
