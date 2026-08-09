"""
Unit tests for the Great Expectations validator (validate_sources.py).

Uses the committed CI fixture CSVs in tests/fixtures/ to test
the full GE validation flow without requiring the real Olist data.

Run:
    pytest tests/test_validate_sources.py -v
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def test_fixtures_directory_exists():
    """Fixtures must be present for CI GE testing to work."""
    assert FIXTURES_DIR.is_dir(), f"Fixtures directory not found: {FIXTURES_DIR}"


def test_all_fixture_csvs_present():
    """All 4 fixture CSVs committed to the repo must be present."""
    expected = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_order_items_dataset.csv",
    ]
    for filename in expected:
        path = FIXTURES_DIR / filename
        assert path.exists(), f"Missing fixture: {path}"


def test_all_fixture_suites_present():
    """All 4 CI expectation suite JSON files must be present."""
    expected = [
        "orders_suite.json",
        "customers_suite.json",
        "products_suite.json",
        "order_items_suite.json",
    ]
    for filename in expected:
        path = FIXTURES_DIR / filename
        assert path.exists(), f"Missing CI suite: {path}"


def test_fixture_csvs_have_expected_columns():
    """Spot-check that fixture CSVs have the schema expected by the suites."""
    import pandas as pd

    checks = {
        "olist_orders_dataset.csv": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        "olist_customers_dataset.csv": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
        "olist_products_dataset.csv": ["product_id", "product_category_name"],
        "olist_order_items_dataset.csv": ["order_id", "order_item_id", "product_id", "price", "freight_value"],
    }
    for csv_file, required_cols in checks.items():
        df = pd.read_csv(FIXTURES_DIR / csv_file)
        for col in required_cols:
            assert col in df.columns, f"{csv_file} missing column '{col}'"


def test_validate_sources_passes_on_fixtures():
    """
    Run the full GE validator against the committed fixture data.
    Should exit with 0 (all suites pass) since fixtures are designed to pass.
    """
    import subprocess, sys
    result = subprocess.run(
        [
            sys.executable,
            "great_expectations/validate_sources.py",
            "--data-dir", str(FIXTURES_DIR),
            "--suites-dir", str(FIXTURES_DIR),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    assert result.returncode == 0, (
        f"GE validator failed on fixtures.\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def test_validate_sources_fails_on_bad_data(tmp_path):
    """
    Validator should exit with 1 when given a CSV that fails expectations
    (e.g., a null order_id which violates expect_column_values_to_not_be_null).
    """
    import subprocess, sys, shutil

    # Copy fixture suite files to tmp_path
    for suite_file in FIXTURES_DIR.glob("*.json"):
        shutil.copy(suite_file, tmp_path / suite_file.name)

    # Write a bad orders CSV: order_id contains nulls
    bad_orders = tmp_path / "olist_orders_dataset.csv"
    bad_orders.write_text(
        "order_id,customer_id,order_status,order_purchase_timestamp\n"
        ",cust_001,delivered,2024-01-15 10:30:00\n"  # null order_id
    )

    # Use fixture CSVs for the other 3 datasets (copy them)
    for csv_file in FIXTURES_DIR.glob("*.csv"):
        if csv_file.name != "olist_orders_dataset.csv":
            shutil.copy(csv_file, tmp_path / csv_file.name)

    result = subprocess.run(
        [
            sys.executable,
            "great_expectations/validate_sources.py",
            "--data-dir", str(tmp_path),
            "--suites-dir", str(tmp_path),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    assert result.returncode == 1, (
        "GE validator should have returned exit code 1 for bad data, "
        f"but returned {result.returncode}"
    )
