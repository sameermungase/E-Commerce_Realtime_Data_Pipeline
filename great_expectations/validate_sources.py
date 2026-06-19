"""
Great Expectations — Source Data Validator

Pre-ETL data quality gate: validates all 4 raw Olist CSV files
against their expectation suites before they enter the pipeline.

Usage:
    python great_expectations/validate_sources.py

Exit Codes:
    0 — All suites passed
    1 — One or more suites failed (pipeline should halt)

Architecture:
    CSV Files → Great Expectations Validation → Pass/Fail Report
    (Runs BEFORE spark_etl.py)
"""

import json
import sys
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset import PandasDataset

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "olist"
EXPECTATIONS_DIR = Path(__file__).resolve().parent / "expectations"

# ──────────────────────────────────────────────
# Dataset → Suite Mapping
# ──────────────────────────────────────────────
VALIDATION_CONFIG = {
    "orders": {
        "csv_file": "olist_orders_dataset.csv",
        "suite_file": "orders_suite.json",
    },
    "customers": {
        "csv_file": "olist_customers_dataset.csv",
        "suite_file": "customers_suite.json",
    },
    "products": {
        "csv_file": "olist_products_dataset.csv",
        "suite_file": "products_suite.json",
    },
    "order_items": {
        "csv_file": "olist_order_items_dataset.csv",
        "suite_file": "order_items_suite.json",
    },
}


def load_suite(suite_path: Path) -> list:
    """Load expectations from a JSON suite file."""
    with open(suite_path, "r") as f:
        suite_data = json.load(f)
    return suite_data.get("expectations", [])


def validate_dataset(name: str, config: dict) -> dict:
    """
    Validate a single CSV dataset against its expectation suite.

    Args:
        name: Dataset name (e.g., 'orders')
        config: Dict with 'csv_file' and 'suite_file' keys

    Returns:
        Dict with 'name', 'success', 'total', 'passed', 'failed', 'details'
    """
    csv_path = DATA_DIR / config["csv_file"]
    suite_path = EXPECTATIONS_DIR / config["suite_file"]

    print(f"\n{'─' * 60}")
    print(f"  Validating: {name}")
    print(f"  CSV:        {csv_path.name}")
    print(f"  Suite:      {suite_path.name}")
    print(f"{'─' * 60}")

    # Check if files exist
    if not csv_path.exists():
        print(f"  ⚠️  CSV file not found: {csv_path}")
        return {
            "name": name,
            "success": False,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "details": "CSV file not found",
        }

    if not suite_path.exists():
        print(f"  ⚠️  Suite file not found: {suite_path}")
        return {
            "name": name,
            "success": False,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "details": "Suite file not found",
        }

    # Load CSV into PandasDataset
    df = pd.read_csv(csv_path)
    ge_df = PandasDataset(df)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

    # Load and run expectations
    expectations = load_suite(suite_path)
    total = len(expectations)
    passed = 0
    failed = 0
    failure_details = []

    for exp in expectations:
        exp_type = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})

        try:
            result = getattr(ge_df, exp_type)(**kwargs)
            if result["success"]:
                passed += 1
                print(f"  ✅ {exp_type}")
            else:
                failed += 1
                detail = f"{exp_type} — kwargs: {kwargs}"
                failure_details.append(detail)
                print(f"  ❌ {exp_type}")
        except Exception as e:
            failed += 1
            detail = f"{exp_type} — ERROR: {e}"
            failure_details.append(detail)
            print(f"  ❌ {exp_type} (error: {e})")

    success = failed == 0
    status = "PASSED ✅" if success else "FAILED ❌"
    print(f"\n  Result: {status} ({passed}/{total} expectations passed)")

    return {
        "name": name,
        "success": success,
        "total": total,
        "passed": passed,
        "failed": failed,
        "details": failure_details,
    }


def main():
    print("=" * 60)
    print("  Great Expectations — Source Data Validation")
    print("  Pre-ETL Quality Gate")
    print("=" * 60)

    results = []
    for name, config in VALIDATION_CONFIG.items():
        result = validate_dataset(name, config)
        results.append(result)

    # ── Summary Report ─────────────────────────
    print("\n")
    print("=" * 60)
    print("  VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  {'Dataset':<15} {'Status':<10} {'Passed':<8} {'Failed':<8} {'Total':<8}")
    print(f"  {'─' * 49}")

    all_passed = True
    for r in results:
        status = "✅ PASS" if r["success"] else "❌ FAIL"
        print(f"  {r['name']:<15} {status:<10} {r['passed']:<8} {r['failed']:<8} {r['total']:<8}")
        if not r["success"]:
            all_passed = False

    print(f"  {'─' * 49}")

    if all_passed:
        print("\n  🎉 All validation suites PASSED — safe to proceed with ETL")
        print("=" * 60)
        sys.exit(0)
    else:
        print("\n  🚨 One or more suites FAILED — ETL pipeline should NOT proceed")
        print("\n  Failed expectations:")
        for r in results:
            if not r["success"] and isinstance(r["details"], list):
                for detail in r["details"]:
                    print(f"    • [{r['name']}] {detail}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
