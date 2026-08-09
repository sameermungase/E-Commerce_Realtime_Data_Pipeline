# run_pipeline.ps1
# Runs the full ETL pipeline locally (outside Airflow)
# Order: Validate → ETL → dbt run → dbt test
#
# Usage: .\scripts\run_pipeline.ps1

$ErrorActionPreference = "Stop"

# ── Paths ──
$ProjectDir = Split-Path -Parent $PSScriptRoot
$VenvPython = "$ProjectDir\.venv\Scripts\python.exe"
$DbtExe     = "$ProjectDir\.venv\Scripts\dbt.exe"
$DbtProject = "$ProjectDir\dbt\ecommerce_dbt"

# ── Validate JAVA_HOME ──
if (-not $env:JAVA_HOME) {
    Write-Host "ERROR: JAVA_HOME is not set." -ForegroundColor Red
    Write-Host "Set it to your Java 21 installation path. See .env.example" -ForegroundColor Red
    exit 1
}
Write-Host "JAVA_HOME = $env:JAVA_HOME" -ForegroundColor Cyan

# ── Step 1: Validate Sources (Great Expectations) ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 1: Validating Source Data (GE)" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $VenvPython "$ProjectDir\great_expectations\validate_sources.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Validation failed! Pipeline halted." -ForegroundColor Red
    exit 1
}

# ── Step 2: Run PySpark ETL ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 2: Running PySpark ETL" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $VenvPython "$ProjectDir\batch\spark_etl.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ETL failed!" -ForegroundColor Red
    exit 1
}

# ── Step 3: Run dbt models ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 3: Running dbt models" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $DbtExe run --project-dir $DbtProject --profiles-dir $DbtProject
if ($LASTEXITCODE -ne 0) {
    Write-Host "dbt run failed!" -ForegroundColor Red
    exit 1
}

# ── Step 4: Run dbt tests ──
Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Step 4: Running dbt tests" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

& $DbtExe test --project-dir $DbtProject --profiles-dir $DbtProject
if ($LASTEXITCODE -ne 0) {
    Write-Host "dbt test failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host " Pipeline Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
