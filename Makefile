# ================================================================
# E-Commerce Real-Time Data Pipeline — Makefile
# ================================================================
# Cross-platform: works on Linux, macOS, and Windows (Git Bash/WSL).
#
# Usage:
#   make help        Show all available commands
#   make infra       Start Docker infrastructure
#   make etl         Run PySpark batch ETL
#   make test        Run all tests (dbt + Great Expectations)
# ================================================================

.PHONY: help infra infra-down etl etl-s3 pipeline stream-producer stream-consumer \
        validate dbt-run dbt-run-athena dbt-test dbt-docs dbt-deps \
        lint lint-sql lint-python test unit-test clean

# ── Cross-platform ───────────────────────────────────────────
# Detect OS: set RM and RMDIR to cross-platform equivalents
ifeq ($(OS),Windows_NT)
    RM    = del /q /f
    RMDIR = rmdir /s /q
    SEP   = \\
else
    RM    = rm -f
    RMDIR = rm -rf
    SEP   = /
endif

# ── Default ──────────────────────────────────────────────────
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ──────────────────────────────────────────
infra: ## Start Docker infrastructure (PostgreSQL + Kafka + Airflow)
	docker compose up -d
	@echo "[OK] Infrastructure started"

infra-down: ## Stop Docker infrastructure
	docker compose down
	@echo "[OK] Infrastructure stopped"

# ── Data Quality ────────────────────────────────────────────
validate: ## Run Great Expectations validation on raw CSVs
	python great_expectations/validate_sources.py

# ── Batch Pipeline ──────────────────────────────────────────
etl: ## Run PySpark batch ETL (PostgreSQL only)
	python batch/spark_etl.py

etl-s3: ## Run PySpark batch ETL (PostgreSQL + S3 Lakehouse)
	ENABLE_AWS_LAKEHOUSE=true python batch/spark_etl.py

etl-debug: ## Run ETL with row-count logging enabled
	ETL_DEBUG=true python batch/spark_etl.py

# ── Full Pipeline ───────────────────────────────────────────
pipeline: validate etl dbt-run dbt-test ## Run full pipeline: Validate → ETL → dbt run → dbt test
	@echo ""
	@echo "[OK] Full pipeline complete!"

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

dbt-run-athena: ## Run all dbt models (Athena)
	dbt run --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt --target athena

dbt-test: ## Run dbt tests
	dbt test --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt

dbt-docs: ## Generate and serve dbt documentation (lineage graph + exposures)
	dbt docs generate --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt
	@echo "[OK] Docs generated — opening browser..."
	dbt docs serve --project-dir dbt/ecommerce_dbt --profiles-dir dbt/ecommerce_dbt

# ── Linting ─────────────────────────────────────────────────
lint: lint-sql lint-python ## Run all linters

lint-sql: ## Run SQLFluff on dbt models
	sqlfluff lint dbt/ecommerce_dbt/models/ --dialect postgres --ignore templating

lint-python: ## Run Flake8 on Python code
	flake8 batch/ streaming/ great_expectations/ airflow/ config/ \
		--max-line-length 120 \
		--extend-ignore E501,W503 \
		--exclude __pycache__,checkpoints

# ── Testing ─────────────────────────────────────────────────
test: validate dbt-test unit-test ## Run all tests (GE + dbt + unit)
	@echo ""
	@echo "[OK] All tests passed!"

unit-test: ## Run Python unit tests (pytest)
	pytest tests/ -v --tb=short

# ── Cleanup ─────────────────────────────────────────────────
clean: ## Stop Docker and clean temporary files
	docker compose down -v
	$(RMDIR) dbt$(SEP)ecommerce_dbt$(SEP)target    2>/dev/null || true
	$(RMDIR) dbt$(SEP)ecommerce_dbt$(SEP)logs      2>/dev/null || true
	$(RMDIR) streaming$(SEP)checkpoints             2>/dev/null || true
	$(RMDIR) logs                                   2>/dev/null || true
	@echo "[OK] Cleaned up"
