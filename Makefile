# ================================================================
# E-Commerce Real-Time Data Pipeline — Makefile
# ================================================================
# Common commands for development, testing, and operations.
#
# Usage:
#   make help        Show all available commands
#   make infra       Start Docker infrastructure
#   make etl         Run PySpark batch ETL
#   make test        Run all tests (dbt + Great Expectations)
# ================================================================

.PHONY: help infra infra-down etl etl-bq stream validate \
        dbt-run dbt-test dbt-docs dbt-deps \
        lint lint-sql lint-python test clean

# ── Default ──────────────────────────────────────────────────
help: ## Show this help message
	@echo.
	@echo   E-Commerce Real-Time Data Pipeline
	@echo   ===================================
	@echo.
	@echo   Available commands:
	@echo.
	@for /F "tokens=1,2* delims=#" %%a in ('findstr /R "^[a-z][a-z-]*:.*##" Makefile') do @echo     %%a %%b
	@echo.

# ── Infrastructure ──────────────────────────────────────────
infra: ## Start Docker infrastructure (PostgreSQL + Kafka)
	docker compose up -d
	@echo [OK] Infrastructure started

infra-down: ## Stop Docker infrastructure
	docker compose down
	@echo [OK] Infrastructure stopped

# ── Data Quality ────────────────────────────────────────────
validate: ## Run Great Expectations validation on raw CSVs
	python great_expectations/validate_sources.py

# ── Batch Pipeline ──────────────────────────────────────────
etl: ## Run PySpark batch ETL (PostgreSQL only)
	python batch/spark_etl.py

etl-bq: ## Run PySpark batch ETL (PostgreSQL + BigQuery)
	set ENABLE_BIGQUERY=true && python batch/spark_etl.py

# ── Streaming Pipeline ──────────────────────────────────────
stream-producer: ## Start Kafka order event producer
	python streaming/kafka_producer.py

stream-consumer: ## Start Spark Structured Streaming consumer
	python streaming/spark_streaming.py

# ── dbt ─────────────────────────────────────────────────────
dbt-deps: ## Install dbt packages (dbt-utils)
	dbt deps --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt

dbt-run: ## Run all dbt models (PostgreSQL)
	dbt run --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt --target dev

dbt-run-bq: ## Run all dbt models (BigQuery)
	dbt run --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt --target bigquery

dbt-test: ## Run dbt tests
	dbt test --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt

dbt-docs: ## Generate and serve dbt documentation (lineage graph)
	dbt docs generate --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt
	@echo [OK] Docs generated — opening browser...
	dbt docs serve --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt

# ── Linting ─────────────────────────────────────────────────
lint: lint-sql lint-python ## Run all linters

lint-sql: ## Run SQLFluff on dbt models
	sqlfluff lint dbt/ecommerce_dbt/models/ --dialect postgres --ignore templating

lint-python: ## Run Flake8 on Python code
	flake8 batch/ streaming/ great_expectations/ --max-line-length 120 --extend-ignore E501,W503 --exclude __pycache__,checkpoints

# ── Testing ─────────────────────────────────────────────────
test: validate dbt-test ## Run all tests (Great Expectations + dbt)
	@echo.
	@echo [OK] All tests passed!

# ── Cleanup ─────────────────────────────────────────────────
clean: ## Stop Docker and clean temporary files
	docker compose down -v
	if exist "dbt\ecommerce_dbt\target" rmdir /s /q "dbt\ecommerce_dbt\target"
	if exist "dbt\ecommerce_dbt\logs" rmdir /s /q "dbt\ecommerce_dbt\logs"
	if exist "streaming\checkpoints" rmdir /s /q "streaming\checkpoints"
	if exist "logs" rmdir /s /q "logs"
	@echo [OK] Cleaned up
