#!/usr/bin/bash

SERVICE_NAME=spark-dev
SPARK_PACKAGES = org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,software.amazon.awssdk:bundle:2.17.257,org.apache.hadoop:hadoop-aws:3.3.4
SPARK_SUBMIT_CMD = spark-submit --packages $(SPARK_PACKAGES)

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
	@echo "  make clean         	- Remove __pycache__ and .pyc files"
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
	@echo "--> Building Docker environment (dependencies will be baked into the image)..."
	docker-compose up -d --build
	@echo "--> Environment is ready!"

up:
	@echo "--> Starting Docker services..."
	docker-compose up -d

down:
	@echo "--> Stopping Docker services..."
	docker-compose down

logs:
	@echo "--> Following logs..."
	docker-compose logs -f

shell:
	@echo "--> Accessing container shell..."
	docker-compose exec $(SERVICE_NAME) bash

clean:
	@echo "--> Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

linter:
	@echo "--> Linting and formatting code with Ruff..."
	ruff check . --fix && ruff format .

bronze:
	@echo "--> STEP 1: Ingesting raw data to Bronze layer..."
	docker-compose exec $(SERVICE_NAME) python src/ingest_to_bronze.py

silver:
	@echo "--> STEP 2: Processing data from Bronze to Silver layer..."
	docker-compose exec $(SERVICE_NAME) $(SPARK_SUBMIT_CMD) src/process_to_silver.py

gold:
	@echo "--> STEP 3: Building aggregated Gold layer tables..."
	docker-compose exec $(SERVICE_NAME) $(SPARK_SUBMIT_CMD) src/build_gold_layer.py

run_full_pipeline: bronze silver gold
	@echo "--> Full ETL pipeline finished successfully."