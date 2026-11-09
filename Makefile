# ============================================================
# VARIABLES
# ============================================================
# Spark cluster sizing. Override on the command line, e.g.:
#   make up-demo WORKERS=3 WORKER_CORES=2 WORKER_MEMORY=3G
# See docs/cluster-sizing.md (or run `make sizing`) to pick values for your machine.
# Make does not remember previous values: pass every non-default value each time.
#   WORKERS       number of spark-worker containers
#   WORKER_CORES  task slots each worker offers to Spark (not a CPU reservation)
#   WORKER_MEMORY memory each worker can allocate to Spark drivers/executors
WORKERS ?= 2
WORKER_CORES ?= 3
WORKER_MEMORY ?= 2G

# docker compose substitutes ${SPARK_WORKER_CORES} / ${SPARK_WORKER_MEMORY} in
# docker-compose.yml from the environment. Exporting them here makes the
# Makefile the single source of truth for worker sizing.
export SPARK_WORKER_CORES  := $(WORKER_CORES)
export SPARK_WORKER_MEMORY := $(WORKER_MEMORY)

COMPOSE_FILE := docker/docker-compose.yml
PROJECT_NAME := distributed-api-etl
ALL_PROFILES := --profile jupyter --profile history --profile keycloak

# ============================================================
# DOCKER COMPOSE COMMANDS
# ============================================================
.PHONY: up up-jupyter up-history up-keycloak up-all up-demo down restart logs ps

up:  ## Start the core services (accepts WORKERS, WORKER_CORES, WORKER_MEMORY)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) up -d --scale spark-worker=$(WORKERS)

up-jupyter:  ## Start all services including Jupyter
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter up -d --scale spark-worker=$(WORKERS)

up-history:  ## Start all services including Spark History Server
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile history up -d --scale spark-worker=$(WORKERS)

up-keycloak:  ## Start all services including Keycloak OAuth2 server and mock-api
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile keycloak up -d --scale spark-worker=$(WORKERS)

up-demo:  ## Start all services for running demos (Jupyter + Keycloak + mock-api)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile keycloak up -d --scale spark-worker=$(WORKERS)

up-all:  ## Start all services (Jupyter, History Server, Keycloak)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history --profile keycloak up -d --scale spark-worker=$(WORKERS)

down:  ## Stop and remove all containers (data volumes are kept)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) $(ALL_PROFILES) down

restart:  ## Restart running containers in place (keeps profiles and worker count)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) $(ALL_PROFILES) restart

logs:  ## Tail logs (SERVICE=name for specific service)
ifdef SERVICE
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) logs -f $(SERVICE)
else
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) logs -f
endif

ps:  ## Show running containers
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) ps

# ============================================================
# SPARK COMMANDS
# ============================================================
.PHONY: spark-shell spark-submit spark-sql driver-logs

driver-logs:  ## Show Spark driver logs of an Airflow job (ID=driver-... ; default: latest)
	@for c in $$(docker ps --filter "label=com.docker.compose.project=$(PROJECT_NAME)" \
			--filter "label=com.docker.compose.service=spark-worker" --format '{{.Names}}'); do \
		for d in $$(docker exec $$c sh -c 'ls -d /opt/spark/work/driver-* 2>/dev/null'); do \
			echo "$$(basename $$d) $$c"; \
		done; \
	done | sort | { if [ -n "$(ID)" ]; then grep "^$(ID) "; else tail -1; fi; } | \
	{ read -r drv c || { echo "No driver logs found$(if $(ID), for $(ID))."; exit 1; }; \
		echo "== $$drv on $$c: stdout (Python output)"; docker exec $$c cat /opt/spark/work/$$drv/stdout; \
		echo "== $$drv on $$c: stderr (last 40 lines; full log: docker exec $$c cat /opt/spark/work/$$drv/stderr)"; \
		docker exec $$c tail -40 /opt/spark/work/$$drv/stderr; }

spark-shell:  ## Open PySpark shell
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec spark-master \
		/opt/spark/bin/pyspark --master spark://spark-master:7077

spark-submit:  ## Submit Spark job (APP=path/to/script.py)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec spark-master \
		/opt/spark/bin/spark-submit --master spark://spark-master:7077 $(APP)

spark-sql:  ## Open Spark SQL shell
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec spark-master \
		/opt/spark/bin/spark-sql --master spark://spark-master:7077

# ============================================================
# AIRFLOW COMMANDS
# ============================================================
.PHONY: dag-list dag-trigger dag-status dag-logs

dag-list:  ## List all available DAGs
	@docker exec airflow-scheduler airflow dags list -o plain

dag-trigger:  ## Trigger a DAG, unpausing it first (DAG=dag_id)
ifndef DAG
	@echo "Usage: make dag-trigger DAG=<dag_id>"
	@echo "Available DAGs:"
	@docker exec airflow-scheduler airflow dags list -o plain | tail -n +2 | awk '{print "  - " $$1}'
else
	docker exec airflow-scheduler airflow dags unpause $(DAG)
	docker exec airflow-scheduler airflow dags trigger $(DAG)
endif

dag-status:  ## Show recent runs (DAG=dag_id for one DAG; RUN=run_id adds task states)
ifdef DAG
	@docker exec airflow-scheduler airflow dags list-runs -d $(DAG) --no-backfill -o plain \
		| awk 'NR == 1 || NR <= 6 {printf "%-36s %-10s %s\n", $$2, $$3, $$5}'
ifdef RUN
	@echo ""
	@docker exec airflow-scheduler airflow tasks states-for-dag-run $(DAG) "$(RUN)" -o plain \
		| awk '{printf "%-42s %s\n", $$3, $$4}'
endif
else
	@for dag in $$(docker exec airflow-scheduler airflow dags list -o plain | tail -n +2 | awk '{print $$1}'); do \
		docker exec airflow-scheduler airflow dags list-runs -d $$dag --no-backfill -o plain \
			| awk -v d=$$dag 'NR == 2 {printf "%-42s %-10s %s\n", d, $$3, $$2} END {if (NR < 2) printf "%-42s %s\n", d, "(no runs)"}'; \
	done
endif

dag-logs:  ## Show the latest log of a task (DAG=dag_id TASK=task_id [RUN=run_id])
ifndef DAG
	@echo "Usage: make dag-logs DAG=<dag_id> TASK=<task_id> [RUN=<run_id>]"
else ifndef TASK
	@echo "Usage: make dag-logs DAG=<dag_id> TASK=<task_id> [RUN=<run_id>]"
	@echo "Tasks in $(DAG):"
	@docker exec airflow-scheduler airflow tasks list $(DAG)
else
	@docker exec airflow-scheduler sh -c 'f=$$(ls -t /opt/airflow/logs/dag_id=$(DAG)/run_id=$(or $(RUN),*)/task_id=$(TASK)/attempt=*.log 2>/dev/null | head -1); \
		if [ -z "$$f" ]; then echo "No logs found for $(DAG) / $(TASK)"; exit 1; fi; echo "== $$f"; cat "$$f"'
endif

# ============================================================
# DEVELOPMENT COMMANDS
# ============================================================
.PHONY: install requirements test test-unit test-integration lint format typecheck

install:  ## Create/update the local Python environment (.venv) with uv
	uv sync

requirements:  ## Regenerate requirements.txt from uv.lock (for pip users)
	uv export --no-hashes --format requirements-txt -o requirements.txt

test:  ## Run pytest
	uv run pytest tests/ -v

test-unit:  ## Run unit tests only
	uv run pytest tests/unit/ -v

test-integration:  ## Run integration tests only
	uv run pytest tests/integration/ -v

lint:  ## Run linter
	uv run ruff check src/ tests/

format:  ## Format code
	uv run ruff format src/ tests/

typecheck:  ## Run type checker
	uv run pyright src/

# ============================================================
# BUILD COMMANDS
# ============================================================
.PHONY: build build-spark build-airflow build-jupyter build-mock-api build-demo

build:  ## Build all required images (Spark, Airflow, Hive)
	$(MAKE) build-spark
	$(MAKE) build-airflow
	$(MAKE) build-hive

build-demo:  ## Build all images needed for demos (includes Jupyter and mock-api)
	$(MAKE) build
	$(MAKE) build-jupyter
	$(MAKE) build-mock-api

build-spark:  ## Build custom Spark image
	docker build -t $(PROJECT_NAME)-spark:latest -f docker/spark/Dockerfile .

build-airflow:  ## Build custom Airflow image
	docker build -t $(PROJECT_NAME)-airflow:latest -f docker/airflow/Dockerfile .

build-hive:  ## Build custom Hive metastore image
	docker build -t $(PROJECT_NAME)-hive:latest -f docker/hive/Dockerfile .

build-jupyter:  ## Build custom Jupyter image
	docker build -t $(PROJECT_NAME)-jupyter:latest -f docker/jupyter/Dockerfile .

build-mock-api:  ## Build mock API image for demo authentication testing
	docker build -t $(PROJECT_NAME)-mock-api:latest -f docker/mock-api/Dockerfile docker/mock-api

# ============================================================
# CLEANUP COMMANDS
# ============================================================
.PHONY: clean clean-volumes clean-all

clean:  ## Stop containers and remove networks
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) $(ALL_PROFILES) down --remove-orphans

clean-volumes:  ## Remove volumes (WARNING: deletes data)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) $(ALL_PROFILES) down -v

clean-all:  ## Full cleanup including images
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) $(ALL_PROFILES) down -v --rmi local

# ============================================================
# UTILITY COMMANDS
# ============================================================
.PHONY: help shell sizing

sizing:  ## Suggest WORKERS / WORKER_CORES / WORKER_MEMORY for this machine
	@bash docker/cluster-sizing.sh

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Cluster parameters (pass to any up-* target, e.g. make up-demo WORKERS=3):"
	@printf "  \033[36m%-18s\033[0m %-6s %s\n" WORKERS "$(WORKERS)" "number of Spark worker containers"
	@printf "  \033[36m%-18s\033[0m %-6s %s\n" WORKER_CORES "$(WORKER_CORES)" "task slots per worker"
	@printf "  \033[36m%-18s\033[0m %-6s %s\n" WORKER_MEMORY "$(WORKER_MEMORY)" "memory per worker for Spark drivers/executors"
	@echo "  Run 'make sizing' or see docs/cluster-sizing.md to choose values for your machine."

shell:  ## Open shell in container (CONTAINER=name)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec $(CONTAINER) /bin/bash

.DEFAULT_GOAL := help
