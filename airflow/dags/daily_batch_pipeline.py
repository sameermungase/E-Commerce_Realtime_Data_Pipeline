"""
Airflow DAG: daily_batch_pipeline

Orchestrates the E-Commerce ETL pipeline:
  1. validate_sources — Runs Great Expectations data quality checks on raw CSVs
  2. spark_etl        — Runs PySpark cleaning and loads raw tables (PostgreSQL + S3 Lakehouse)
  3. dbt_run          — Runs dbt models for analytics star-schema
  4. dbt_test         — Runs dbt tests to validate model integrity
"""

import json as _json
import logging
import os
import platform
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Paths — resolved from environment variables
# ──────────────────────────────────────────────
PROJECT_DIR = os.environ.get(
    "PROJECT_DIR",
    str(Path(__file__).resolve().parent.parent.parent),
)

# OS-aware Python path default: Works on Windows (venv) and Linux (Docker)
_is_windows = platform.system() == "Windows"
_default_python = (
    str(Path(PROJECT_DIR) / ".venv" / "Scripts" / "python.exe")
    if _is_windows
    else str(Path(PROJECT_DIR) / ".venv" / "bin" / "python")
)
VENV_PYTHON = os.environ.get("VENV_PYTHON", _default_python)

# OS-aware dbt path default
_default_dbt = (
    str(Path(PROJECT_DIR) / ".venv" / "Scripts" / "dbt.exe")
    if _is_windows
    else str(Path(PROJECT_DIR) / ".venv" / "bin" / "dbt")
)
DBT_EXE = os.environ.get("DBT_EXE", _default_dbt)
DBT_PROJECT_DIR = str(Path(PROJECT_DIR) / "dbt" / "ecommerce_dbt")


# ──────────────────────────────────────────────
# Task Functions
# ──────────────────────────────────────────────

def run_validate_sources():
    """Execute Great Expectations data quality validation on raw CSVs."""
    script_path = os.path.join(PROJECT_DIR, "great_expectations", "validate_sources.py")
    python_bin = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable

    data_dir = os.path.join(PROJECT_DIR, "data", "olist")
    fixtures_dir = os.path.join(PROJECT_DIR, "tests", "fixtures")

    cmd = [python_bin, script_path]
    if not os.path.exists(data_dir) or not os.listdir(data_dir):
        cmd.extend(["--data-dir", fixtures_dir, "--suites-dir", fixtures_dir])

    print(f"Running Great Expectations: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def run_spark_etl():
    """Execute PySpark ETL via subprocess with explicit JAVA_HOME."""
    env = os.environ.copy()
    if not _is_windows or not env.get("JAVA_HOME") or "C:" in env.get("JAVA_HOME", ""):
        env["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        env["POSTGRES_HOST"] = "postgres"
        env["POSTGRES_PORT"] = "5432"

    script_path = os.path.join(PROJECT_DIR, "batch", "spark_etl.py")
    python_bin = VENV_PYTHON if os.path.exists(VENV_PYTHON) else sys.executable
    print(f"Running Spark ETL: {python_bin} {script_path}")

    subprocess.run(
        [python_bin, script_path],
        env=env,
        check=True
    )


def run_dbt_models():
    """Execute dbt run via subprocess."""
    env = os.environ.copy()
    if not _is_windows:
        env["POSTGRES_HOST"] = "postgres"
        env["POSTGRES_PORT"] = "5432"
    dbt_exe = DBT_EXE if os.path.exists(DBT_EXE) else "dbt"
    log_path = "/tmp/dbt_logs"
    target_path = "/tmp/dbt_target"
    print(f"Running dbt: {dbt_exe} run")
    cmd = [
        dbt_exe, "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ]
    if not _is_windows:
        cmd.extend(["--log-path", log_path, "--target-path", target_path])
    subprocess.run(cmd, env=env, check=True)


def run_dbt_tests():
    """Execute dbt test via subprocess to validate model integrity."""
    env = os.environ.copy()
    if not _is_windows:
        env["POSTGRES_HOST"] = "postgres"
        env["POSTGRES_PORT"] = "5432"
    dbt_exe = DBT_EXE if os.path.exists(DBT_EXE) else "dbt"
    log_path = "/tmp/dbt_logs"
    target_path = "/tmp/dbt_target"
    print(f"Running dbt test: {dbt_exe} test")
    cmd = [
        dbt_exe, "test",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ]
    if not _is_windows:
        cmd.extend(["--log-path", log_path, "--target-path", target_path])
    subprocess.run(cmd, env=env, check=True)


# ──────────────────────────────────────────────
# Failure Callback
# ──────────────────────────────────────────────
_log = logging.getLogger("daily_batch_pipeline")


def _on_task_failure(context: dict) -> None:
    """
    Called by Airflow when any task in this DAG fails.

    Logs structured failure details to the Airflow log.
    Optionally posts to Slack if SLACK_WEBHOOK_URL is set in the environment.
    """
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception", "unknown error")

    message = (
        f":red_circle: *Pipeline Failure*\n"
        f"  DAG:  `{dag_id}`\n"
        f"  Task: `{task_id}`\n"
        f"  Run:  `{run_id}`\n"
        f"  Error: {exception}"
    )

    _log.error("TASK FAILURE | dag=%s task=%s run=%s error=%s", dag_id, task_id, run_id, exception)

    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            payload = _json.dumps({"text": message}).encode("utf-8")
            req = urllib.request.Request(slack_url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
            _log.info("Slack notification sent successfully")
        except Exception as exc:
            _log.warning("Failed to send Slack notification: %s", exc)


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="daily_batch_pipeline",
    default_args=default_args,
    description="ETL pipeline: Validate → PySpark → PostgreSQL/S3 Lakehouse → dbt → Test",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "batch", "ecommerce", "data-quality"],
) as dag:

    validate_sources = PythonOperator(
        task_id="validate_sources",
        python_callable=run_validate_sources,
    )

    spark_etl = PythonOperator(
        task_id="spark_etl",
        python_callable=run_spark_etl,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_tests,
    )

    validate_sources >> spark_etl >> dbt_run >> dbt_test
