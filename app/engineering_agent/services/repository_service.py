from __future__ import annotations

import ast
import fnmatch
from dataclasses import dataclass
from pathlib import Path

from app.engineering_agent.utils.security import resolve_within

ALLOWED_SUFFIXES = {".py", ".md", ".json", ".yaml", ".yml", ".toml", ".properties", ".sh", ".txt"}
IGNORED_PARTS = {".git", ".venv", ".agent-venv", "__pycache__", "build", ".ruff_cache", "dependencies", ".pytest_cache"}
RUNTIME_TOKENS = ("SparkContext", "GlueContext", "boto3.client", "boto3.resource", "getResolvedOptions", "Job(")


@dataclass
class RepositoryService:
    repository_root: Path
    generated_tests_root: Path
    max_file_chars: int = 60_000

    def _resolve(self, relative_path: str) -> Path:
        return resolve_within(self.repository_root, relative_path)

    def list_files(self, pattern: str = "*.py", limit: int = 200) -> list[str]:
        results: list[str] = []
        for path in self.repository_root.rglob("*"):
            if not path.is_file() or any(part in IGNORED_PARTS for part in path.parts):
                continue
            relative = path.relative_to(self.repository_root).as_posix()
            if fnmatch.fnmatch(relative, pattern) or fnmatch.fnmatch(path.name, pattern):
                results.append(relative)
            if len(results) >= limit:
                break
        return sorted(results)

    def read_file(self, relative_path: str) -> str:
        path = self._resolve(relative_path)
        if not path.is_file():
            raise FileNotFoundError(relative_path)
        if path.suffix.lower() not in ALLOWED_SUFFIXES:
            raise ValueError(f"Unsupported file type: {path.suffix}")
        return path.read_text(encoding="utf-8", errors="replace")[: self.max_file_chars]

    def search(self, query: str, pattern: str = "*.py", limit: int = 50) -> list[dict[str, object]]:
        needle = query.strip().lower()
        if not needle:
            raise ValueError("Query must not be blank.")
        matches: list[dict[str, object]] = []
        for relative in self.list_files(pattern, 2000):
            for line_number, line in enumerate(self.read_file(relative).splitlines(), 1):
                if needle in line.lower():
                    matches.append({"path": relative, "line": line_number, "text": line.strip()[:300]})
                    if len(matches) >= limit:
                        return matches
        return matches

    def analyse_python(self, relative_path: str) -> dict[str, object]:
        source = self.read_file(relative_path)
        tree = ast.parse(source)
        functions: list[dict[str, object]] = []
        classes: list[dict[str, object]] = []
        imports: list[str] = []
        top_level_calls: list[dict[str, object]] = []
        spark_operations: set[str] = set()
        for node in tree.body:
            rendered = ast.unparse(node)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions.append({"name": node.name, "line": node.lineno, "args": [a.arg for a in node.args.args]})
            elif isinstance(node, ast.ClassDef):
                classes.append({"name": node.name, "line": node.lineno})
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                imports.append(rendered)
            elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.Expr)) and any(token in rendered for token in RUNTIME_TOKENS):
                top_level_calls.append({"line": node.lineno, "code": rendered[:300]})
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in {"join", "filter", "where", "select", "withColumn", "dropDuplicates", "groupBy", "agg", "unionByName"}:
                spark_operations.add(node.attr)
        return {
            "path": relative_path,
            "line_count": len(source.splitlines()),
            "functions": functions,
            "classes": classes,
            "imports": imports,
            "spark_operations": sorted(spark_operations),
            "top_level_runtime_initialisation": top_level_calls,
            "import_safe": not bool(top_level_calls),
        }

    def write_generated_test(self, filename: str, content: str) -> str:
        if Path(filename).name != filename or not filename.startswith("test_") or not filename.endswith(".py"):
            raise ValueError("Generated filename must be a plain test_*.py filename.")
        ast.parse(content)
        target = resolve_within(self.generated_tests_root, filename)
        target.write_text(content, encoding="utf-8")
        return target.relative_to(self.repository_root).as_posix()
