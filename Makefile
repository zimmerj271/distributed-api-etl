# ============================================================
# VARIABLES
# ============================================================
WORKERS ?= 2
COMPOSE_FILE := docker/docker-compose.yml
PROJECT_NAME := distributed-api-etl

# ============================================================
# DOCKER COMPOSE COMMANDS
# ============================================================
.PHONY: up up-jupyter up-history up-all down restart logs ps

up:  ## Start all services (WORKERS=N to scale)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) up -d --scale spark-worker=$(WORKERS)

up-jupyter:  ## Start all services including Jupyter
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter up -d --scale spark-worker=$(WORKERS)

up-history:  ## Start all services including Spark History Server
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile history up -d --scale spark-worker=$(WORKERS)

up-all:  ## Start all services 
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history up -d --scale spark-worker=$(WORKERS)

down:  ## Stop all services
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history down

restart:  ## Restart all services
	$(MAKE) down
	$(MAKE) up

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
.PHONY: spark-shell spark-submit spark-sql

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
# DEVELOPMENT COMMANDS
# ============================================================
.PHONY: test lint format typecheck

test:  ## Run pytest
	pytest tests/ -v

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-integration:  ## Run integration tests only
	pytest tests/integration/ -v

lint:  ## Run linter
	ruff check src/ tests/

format:  ## Format code
	ruff format src/ tests/

typecheck:  ## Run type checker
	pyright src/

# ============================================================
# BUILD COMMANDS
# ============================================================
.PHONY: build build-spark build-airflow build-jupyter

build:  ## Build all custom images
	$(MAKE) build-spark
	$(MAKE) build-airflow
	$(MAKE) build-hive

build-spark:  ## Build custom Spark image
	docker build -t $(PROJECT_NAME)-spark:latest -f docker/spark/Dockerfile .

build-airflow:  ## Build custom Airflow image
	docker build -t $(PROJECT_NAME)-airflow:latest -f docker/airflow/Dockerfile .

build-hive:  ## Build custom Hive metastore image
	docker build -t $(PROJECT_NAME)-hive:latest -f docker/hive/Dockerfile .

build-jupyter:  ## Build custom Jupyter image
	docker build -t $(PROJECT_NAME)-jupyter:latest -f docker/jupyter/Dockerfile .

# ============================================================
# CLEANUP COMMANDS
# ============================================================
.PHONY: clean clean-volumes clean-all

clean:  ## Stop containers and remove networks
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history down --remove-orphans

clean-volumes:  ## Remove volumes (WARNING: deletes data)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history down -v

clean-all:  ## Full cleanup including images
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) --profile jupyter --profile history down -v --rmi local

# ============================================================
# UTILITY COMMANDS
# ============================================================
.PHONY: help shell

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

shell:  ## Open shell in container (CONTAINER=name)
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT_NAME) exec $(CONTAINER) /bin/bash

.DEFAULT_GOAL := help
