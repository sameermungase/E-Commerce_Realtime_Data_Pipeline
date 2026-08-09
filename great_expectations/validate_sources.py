"""
Great Expectations — Source Data Validator (GX 1.x Core API)

Pre-ETL data quality gate: validates all 4 raw Olist CSV files
against their expectation suites before they enter the pipeline.

Usage:
    python great_expectations/validate_sources.py
    python great_expectations/validate_sources.py --data-dir tests/fixtures --suites-dir tests/fixtures

Exit Codes:
    0 — All suites passed
    1 — One or more suites failed (pipeline should halt)

Architecture:
    CSV Files → Great Expectations Validation → Pass/Fail Report
    (Runs BEFORE spark_etl.py)
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

# ──────────────────────────────────────────────
# Paths (defaults, overridable via CLI args)
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "olist"
DEFAULT_EXPECTATIONS_DIR = Path(__file__).resolve().parent / "expectations"

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


def snake_to_pascal(name: str) -> str:
    """Convert snake_case expectation type to PascalCase class name.

    Example: expect_column_values_to_not_be_null → ExpectColumnValuesToNotBeNull
    """
    return "".join(word.capitalize() for word in name.split("_"))


def load_suite(suite_path: Path) -> list:
    """Load expectations from a JSON suite file."""
    with open(suite_path, "r") as f:
        suite_data = json.load(f)
    return suite_data.get("expectations", [])


def validate_dataset(
    context,
    data_source,
    name: str,
    config: dict,
    data_dir: Path,
    suites_dir: Path,
) -> dict:
    """
    Validate a single CSV dataset against its expectation suite.

    Uses the GX 1.x Core API: EphemeralDataContext → PandasDatasource
    → DataFrameAsset → BatchDefinition → ExpectationSuite → ValidationDefinition.

    Args:
        context: GX EphemeralDataContext
        data_source: Pandas data source added to the context
        name: Dataset name (e.g., 'orders')
        config: Dict with 'csv_file' and 'suite_file' keys
        data_dir: Directory containing CSV files
        suites_dir: Directory containing expectation suite JSON files

    Returns:
        Dict with 'name', 'success', 'total', 'passed', 'failed', 'details'
    """
    csv_path = data_dir / config["csv_file"]
    suite_path = suites_dir / config["suite_file"]

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

    # Load CSV into pandas DataFrame
    df = pd.read_csv(csv_path)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

    # Build GX data asset and batch
    asset = data_source.add_dataframe_asset(name=name)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{name}_batch")

    # Load expectations from JSON and build suite
    expectations_json = load_suite(suite_path)
    suite = gx.ExpectationSuite(name=f"{name}_suite")

    for exp in expectations_json:
        exp_type = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        try:
            exp_class = getattr(gxe, snake_to_pascal(exp_type))
            suite.add_expectation(exp_class(**kwargs))
        except AttributeError:
            print(f"  ⚠️  Unknown expectation type: {exp_type} — skipping")

    suite = context.suites.add(suite)

    # Create validation definition and run
    validation_def = gx.ValidationDefinition(
        name=f"validate_{name}",
        data=batch_def,
        suite=suite,
    )
    validation_def = context.validation_definitions.add(validation_def)
    validation_result = validation_def.run(batch_parameters={"dataframe": df})

    # Process results
    total = len(validation_result.results)
    passed = 0
    failed = 0
    failure_details = []

    for r in validation_result.results:
        exp_type = r.expectation_config.type
        if r.success:
            passed += 1
            print(f"  ✅ {exp_type}")
        else:
            failed += 1
            detail = f"{exp_type} — kwargs: {r.expectation_config.kwargs}"
            failure_details.append(detail)
            print(f"  ❌ {exp_type}")

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
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Great Expectations — Source Data Validator"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing CSV files to validate",
    )
    parser.add_argument(
        "--suites-dir",
        type=Path,
        default=DEFAULT_EXPECTATIONS_DIR,
        help="Directory containing expectation suite JSON files",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Great Expectations — Source Data Validation")
    print("  Pre-ETL Quality Gate")
    print("=" * 60)

    # Create ephemeral GX context and pandas data source
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("csv_source")

    results = []
    for name, config in VALIDATION_CONFIG.items():
        result = validate_dataset(
            context, data_source, name, config,
            data_dir=args.data_dir,
            suites_dir=args.suites_dir,
        )
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
