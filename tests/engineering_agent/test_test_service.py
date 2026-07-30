from pathlib import Path
import pytest
from app.engineering_agent.services.test_service import PytestService


def test_validate_python_lists_tests(tmp_path: Path):
    svc = PytestService(tmp_path, tmp_path / "generated_tests")
    result = svc.validate_python("def test_one():\n    assert True\n")
    assert result == {"valid": True, "test_functions": ["test_one"], "count": 1}


def test_pytest_path_is_restricted(tmp_path: Path):
    generated = tmp_path / "generated_tests"; generated.mkdir()
    svc = PytestService(tmp_path, generated)
    with pytest.raises(ValueError): svc.run_pytest("app")


def test_summary_counts_include_skips_and_failures():
    result = PytestService._summary_counts(
        "1 failed, 6 passed, 2 skipped, 1 error in 2.00s"
    )
    assert result["passed"] == 6
    assert result["failed"] == 1
    assert result["skipped"] == 2
    assert result["errors"] == 1


def test_summary_counts_for_skipped_only_suite():
    result = PytestService._summary_counts("1 skipped in 0.01s")
    assert result["passed"] == 0
    assert result["skipped"] == 1
