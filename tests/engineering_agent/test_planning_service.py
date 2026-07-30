from pathlib import Path

from app.engineering_agent.services.planning_service import build_test_plan, generate_scaffold


def test_plan_detects_spark_operations():
    analysis = {
        "import_safe": False,
        "spark_operations": ["join", "dropDuplicates"],
        "functions": [{"name": "transform"}],
    }
    plan = build_test_plan("job.py", analysis)
    assert any(item["scenario"] == "Python syntax" for item in plan)
    assert any(item["scenario"] == "Spark operation: join" for item in plan)


def test_generated_contract_is_executable_pytest(tmp_path: Path):
    target = tmp_path / "job.py"
    target.write_text(
        "def transform(df):\n"
        "    return df.join(df, 'id').dropDuplicates(['id'])\n",
        encoding="utf-8",
    )
    analysis = {
        "spark_operations": ["join", "dropDuplicates"],
        "functions": [{"name": "transform"}],
    }
    name, content = generate_scaffold("job.py", analysis, build_test_plan("job.py", analysis))
    assert name == "test_job_contract.py"
    assert "pytest.skip" not in content
    compile(content, name, "exec")
