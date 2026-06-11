"""
Airflow DAG: daily_batch_pipeline

Orchestrates the E-Commerce ETL pipeline:
  1. spark_etl  — Runs PySpark cleaning and loads raw tables
  2. dbt_run    — Runs dbt models for analytics star-schema
"""

import subprocess
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
PROJECT_DIR = r"C:\Users\snowp\OneDrive\Desktop\Projects\DS_project\files"
VENV_PYTHON = rf"{PROJECT_DIR}\.venv\Scripts\python.exe"
DBT_EXE = rf"{PROJECT_DIR}\.venv\Scripts\dbt.exe"
DBT_PROJECT_DIR = rf"{PROJECT_DIR}\dbt\ecommerce_dbt"
JAVA_HOME = r"C:\Users\snowp\OneDrive\Desktop\Projects\DS_project\Java"

# ──────────────────────────────────────────────
# Task Functions
# ──────────────────────────────────────────────
def run_spark_etl():
    """Execute PySpark ETL via subprocess with explicit JAVA_HOME."""
    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    script_path = os.path.join(PROJECT_DIR, "batch", "spark_etl.py")
    print(f"Running Spark ETL: {VENV_PYTHON} {script_path}")
    
    # We run using subprocess.run to execute natively without shell quote issues
    res = subprocess.run(
        [VENV_PYTHON, script_path],
        env=env,
        check=True
    )
    if res.returncode != 0:
        raise RuntimeError(f"Spark ETL failed with exit code {res.returncode}")

def run_dbt_models():
    """Execute dbt run via subprocess."""
    print(f"Running dbt: {DBT_EXE} run")
    res = subprocess.run(
        [
            DBT_EXE, "run",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR
        ],
        check=True
    )
    if res.returncode != 0:
        raise RuntimeError(f"dbt run failed with exit code {res.returncode}")

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
}

with DAG(
    dag_id="daily_batch_pipeline",
    default_args=default_args,
    description="ETL pipeline: PySpark → PostgreSQL raw tables → dbt star schema",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "batch", "ecommerce"],
) as dag:

    spark_etl = PythonOperator(
        task_id="spark_etl",
        python_callable=run_spark_etl,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    spark_etl >> dbt_run
