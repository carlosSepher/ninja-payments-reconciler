.PHONY: help install install-dev run test lint format clean db-init

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install production dependencies
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

install-dev: ## Install development dependencies
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements-dev.txt

run: ## Run the application
	. venv/bin/activate && uvicorn src.app:app --host 0.0.0.0 --port 8001 --log-level info

test: ## Run tests
	. venv/bin/activate && pytest

lint: ## Run linter
	. venv/bin/activate && ruff check .

format: ## Format code
	. venv/bin/activate && ruff format .

clean: ## Clean up generated files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true

db-init: ## Initialize the database with tables
	@echo "Initializing database..."
	@. venv/bin/activate && psql $${DATABASE_DSN} -f scripts/create_tables.sql
