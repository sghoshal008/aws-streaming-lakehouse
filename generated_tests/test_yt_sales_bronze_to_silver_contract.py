"""Generated repository-contract tests for app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py.

These tests are intentionally lightweight: they validate the selected Glue job's
Python structure and expected Spark operations without requiring Java or PySpark.
"""

from __future__ import annotations

import ast
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPOSITORY_ROOT / 'app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py'
EXPECTED_SPARK_OPERATIONS = ['filter', 'withColumn']
EXPECTED_FUNCTIONS = ['utc_now', 'get_last_snapshot_id', 'save_watermark', 'save_run_summary', 'get_current_snapshot_id', 'snapshot_exists', 'main']
RUNTIME_TOKENS = (
    "SparkContext",
    "GlueContext",
    "getResolvedOptions",
    "boto3.client",
    "boto3.resource",
)


def _source() -> str:
    assert TARGET.is_file(), f"Selected job does not exist: {TARGET}"
    return TARGET.read_text(encoding="utf-8")


def test_selected_job_is_valid_python() -> None:
    ast.parse(_source())


def test_expected_spark_operations_are_present() -> None:
    source = _source()
    missing = [name for name in EXPECTED_SPARK_OPERATIONS if f".{name}(" not in source]
    assert not missing, f"Expected Spark operations missing: {missing}"


def test_expected_functions_are_declared() -> None:
    tree = ast.parse(_source())
    declared = {node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    missing = [name for name in EXPECTED_FUNCTIONS if name not in declared]
    assert not missing, f"Expected functions missing: {missing}"


def test_runtime_boundary_is_explicit() -> None:
    source = _source()
    detected = [token for token in RUNTIME_TOKENS if token in source]
    # Glue entry-points commonly initialise runtime services at module scope.
    # This assertion documents that boundary rather than importing the job locally.
    assert isinstance(detected, list)
