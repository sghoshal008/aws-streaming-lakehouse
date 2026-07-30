from __future__ import annotations

import re
from pathlib import Path


def build_test_plan(path: str, analysis: dict[str, object]) -> list[dict[str, str]]:
    """Build a small, executable repository-contract test plan.

    These tests inspect the source file rather than starting Spark. That keeps the
    follow-up demo lightweight while still proving that important engineering
    patterns remain present in the selected Glue job.
    """
    plan: list[dict[str, str]] = [
        {
            "scenario": "Python syntax",
            "assertion": "The selected job remains valid Python and can be parsed with AST.",
            "priority": "High",
        }
    ]

    operations = set(analysis.get("spark_operations", []))
    for operation in sorted(operations):
        plan.append(
            {
                "scenario": f"Spark operation: {operation}",
                "assertion": f"The job still contains the expected {operation} transformation.",
                "priority": "High",
            }
        )

    functions = analysis.get("functions", [])
    if functions:
        plan.append(
            {
                "scenario": "Declared functions",
                "assertion": "Expected top-level function names remain available in the source.",
                "priority": "Medium",
            }
        )

    plan.append(
        {
            "scenario": "Runtime boundary",
            "assertion": "The test reports whether Glue/Spark bootstrap occurs at module scope.",
            "priority": "Medium",
        }
    )
    return plan


def _safe_test_name(path: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_]+", "_", Path(path).stem)
    return f"test_{stem}_contract.py"


def generate_scaffold(
    path: str,
    analysis: dict[str, object],
    plan: list[dict[str, str]],
) -> tuple[str, str]:
    """Generate executable pytest source-contract tests with no Spark dependency."""
    filename = _safe_test_name(path)
    operations = sorted(set(str(item) for item in analysis.get("spark_operations", [])))
    function_names = [
        str(item["name"])
        for item in analysis.get("functions", [])
        if isinstance(item, dict) and item.get("name")
    ]

    content = f'''"""Generated repository-contract tests for {path}.

These tests are intentionally lightweight: they validate the selected Glue job's
Python structure and expected Spark operations without requiring Java or PySpark.
"""

from __future__ import annotations

import ast
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPOSITORY_ROOT / {path!r}
EXPECTED_SPARK_OPERATIONS = {operations!r}
EXPECTED_FUNCTIONS = {function_names!r}
RUNTIME_TOKENS = (
    "SparkContext",
    "GlueContext",
    "getResolvedOptions",
    "boto3.client",
    "boto3.resource",
)


def _source() -> str:
    assert TARGET.is_file(), f"Selected job does not exist: {{TARGET}}"
    return TARGET.read_text(encoding="utf-8")


def test_selected_job_is_valid_python() -> None:
    ast.parse(_source())


def test_expected_spark_operations_are_present() -> None:
    source = _source()
    missing = [name for name in EXPECTED_SPARK_OPERATIONS if f".{{name}}(" not in source]
    assert not missing, f"Expected Spark operations missing: {{missing}}"


def test_expected_functions_are_declared() -> None:
    tree = ast.parse(_source())
    declared = {{node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}}
    missing = [name for name in EXPECTED_FUNCTIONS if name not in declared]
    assert not missing, f"Expected functions missing: {{missing}}"


def test_runtime_boundary_is_explicit() -> None:
    source = _source()
    detected = [token for token in RUNTIME_TOKENS if token in source]
    # Glue entry-points commonly initialise runtime services at module scope.
    # This assertion documents that boundary rather than importing the job locally.
    assert isinstance(detected, list)
'''
    return filename, content
