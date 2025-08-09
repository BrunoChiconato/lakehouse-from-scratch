#!/usr/bin/bash

SERVICE_NAME=spark-dev
ENV_FILE=--env-file .env

DOCKER_EXEC_CMD = docker compose $(ENV_FILE) exec $(SERVICE_NAME)

SPARK_CMD_PREFIX = spark-submit --packages "$$SPARK_PACKAGES"

.PHONY: help build up down logs shell clean linter bronze silver gold run_full_pipeline

default: help

help:
	@echo "Available commands:"
	@echo "  --- Environment Control ---"
	@echo "  make build         	- Build the Docker image with all dependencies"
	@echo "  make up            	- Start Docker services in the background"
	@echo "  make down          	- Stop and remove Docker containers"
	@echo "  make logs          	- Follow service logs"
	@echo "  make shell         	- Open an interactive shell inside the development container"
	@echo "  make clean         	- Remove __pycache__ and .ruff_cache dirs and .pyc files"
	@echo "  make linter      	- Run linter and formatter (Ruff) on the project"
	@echo ""
	@echo "  --- ETL Pipeline Steps ---"
	@echo "  make bronze 		- (Step 1) Ingest raw data from API to Bronze layer"
	@echo "  make silver		- (Step 2) Process Bronze data into Silver layer (Iceberg table)"
	@echo "  make gold    		- (Step 3) Build aggregated Gold layer tables from Silver layer"
	@echo ""
	@echo "  --- Full Pipeline Execution ---"
	@echo "  make run_full_pipeline 	- Run all ETL steps in sequence (Bronze -> Silver -> Gold)"

build:
	@echo "--> Building Docker environment..."
	docker compose build

up:
	@echo "--> Starting Docker services..."
	docker compose up -d

down:
	@echo "--> Stopping Docker services..."
	docker compose down --volumes

logs:
	@echo "--> Following logs..."
	docker compose logs -f

shell:
	@echo "--> Accessing container shell..."
	$(DOCKER_EXEC_CMD) bash

clean:
	@echo "--> Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +

linter:
	@echo "--> Linting and formatting code with Ruff..."
	ruff check . --fix
	ruff format .

bronze:
	@echo "--> STEP 1: Ingesting raw data to Bronze layer..."
	$(DOCKER_EXEC_CMD) python src/ingestion/main.py

silver:
	@echo "--> STEP 2: Processing data from Bronze to Silver layer..."
	$(DOCKER_EXEC_CMD) bash -c '$(SPARK_CMD_PREFIX) src/transformation/process_to_silver.py'

gold:
	@echo "--> STEP 3: Building aggregated Gold layer tables..."
	$(DOCKER_EXEC_CMD) bash -c '$(SPARK_CMD_PREFIX) src/transformation/build_gold_layer.py'

run_full_pipeline: bronze silver gold
	@echo "--> Full ETL pipeline finished successfully."