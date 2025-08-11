#!/usr/bin/bash

.PHONY: help clean linter test test-cov

default: help

help:
	@echo "Available commands:"
	@echo "  --- Environment Control ---"
	@echo "  make clean         	- Remove __pycache__ and .ruff_cache dirs and .pyc files"
	@echo "  make linter      	- Run linter and formatter (Ruff) on the project"
	@echo ""
	@echo "  --- Testing ---"
	@echo "  make test          	- Run all unit and integration tests with pytest"
	@echo "  make test-cov      	- Run tests and generate an HTML coverage report"


clean:
	@echo "--> Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

linter:
	@echo "--> Linting and formatting code with Ruff..."
	ruff check . --fix
	ruff format .

test:
	@echo "--> Running unit and integration tests..."
	pytest tests/

test-cov:
	@echo "--> Running tests and generating HTML coverage report..."
	pytest --cov=src --cov-report=html tests/
	@echo "--> Coverage report generated in 'htmlcov/' directory. Open htmlcov/index.html in your browser."