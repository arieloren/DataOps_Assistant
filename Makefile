# Makefile
.PHONY: help setup dev-up dev-down airflow-ui test lint clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $1, $2}' $(MAKEFILE_LIST)

setup: ## Install development dependencies locally  
	@echo "Setting up development environment..."
	@pip install -e .
	@pip install black ruff pytest
	@echo "✓ Setup complete"

dev-up: ## Start Airflow development stack
	@echo "Starting Airflow development environment..."
	@cd docker && docker compose up -d --build
	@echo "✓ Airflow started at http://localhost:8080"
	@echo "  Username: airflow"
	@echo "  Password: airflow"

dev-down: ## Stop Airflow development stack
	@echo "Stopping Airflow development environment..."
	@cd docker && docker compose down -v
	@echo "✓ Airflow stopped"

airflow-ui: ## Open Airflow web interface
	@open http://localhost:8080 2>/dev/null || echo "Open http://localhost:8080 in your browser"

test: ## Run tests
	@echo "Running tests..."
	@pytest tests/ -v

lint: ## Run code formatting and linting
	@echo "Running Black formatter..."
	@black app/ tests/
	@echo "Running Ruff linter..."
	@ruff app/ tests/

clean: ## Clean temporary files and data
	@echo "Cleaning temporary files..."
	@rm -rf data/parquet/* data/sqlite/* data/catalog/* /tmp/etl_pipeline/
	@echo "✓ Cleaned"

# Pipeline generation shortcuts
parse: ## Parse natural language prompt (usage: make parse PROMPT="your request")
	@python -m app.a1_parser --prompt "$(PROMPT)"

generate: ## Generate pipeline from spec (usage: make generate SPEC=specs/pipeline.json)  
	@python -m app.a2_generator --spec "$(SPEC)"
