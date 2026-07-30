from pathlib import Path
import pytest
from app.engineering_agent.services.repository_service import RepositoryService


def service(tmp_path: Path) -> RepositoryService:
    generated = tmp_path / "generated_tests"; generated.mkdir()
    return RepositoryService(tmp_path, generated)


def test_blocks_path_traversal(tmp_path: Path):
    with pytest.raises(ValueError): service(tmp_path).read_file("../secret.py")


def test_requires_safe_test_filename(tmp_path: Path):
    with pytest.raises(ValueError): service(tmp_path).write_generated_test("unsafe.py", "def test_x(): pass")


def test_writes_only_generated_test(tmp_path: Path):
    path = service(tmp_path).write_generated_test("test_demo.py", "def test_demo():\n    assert True\n")
    assert path == "generated_tests/test_demo.py"
