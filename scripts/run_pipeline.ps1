# run_pipeline.ps1
# Helper script to run the full ETL pipeline locally (outside Airflow)
# Usage: .\scripts\run_pipeline.ps1

$ErrorActionPreference = "Stop"

# ── Paths ──
$ProjectDir = Split-Path -Parent $PSScriptRoot
$VenvPython = "$ProjectDir\.venv\Scripts\python.exe"
$DbtExe     = "$ProjectDir\.venv\Scripts\dbt.exe"
$DbtProject = "$ProjectDir\dbt\ecommerce_dbt"

# ── Set JAVA_HOME for PySpark ──
$env:JAVA_HOME = "C:\Users\snowp\OneDrive\Desktop\Projects\DS_project\Java"
Write-Host "JAVA_HOME = $env:JAVA_HOME" -ForegroundColor Cyan

# ── Step 1: Run PySpark ETL ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 1: Running PySpark ETL" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $VenvPython "$ProjectDir\batch\spark_etl.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ETL failed!" -ForegroundColor Red
    exit 1
}

# ── Step 2: Run dbt ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 2: Running dbt models" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $DbtExe run --project-dir $DbtProject --profiles-dir $DbtProject
if ($LASTEXITCODE -ne 0) {
    Write-Host "dbt run failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Pipeline Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
