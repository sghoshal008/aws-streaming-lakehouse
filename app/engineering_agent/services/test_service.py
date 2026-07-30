from __future__ import annotations

import ast
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PytestService:
    repository_root: Path
    generated_tests_root: Path
    timeout_seconds: int = 180

    def validate_python(self, content: str) -> dict[str, object]:
        tree = ast.parse(content)
        names = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test_")
        ]
        return {
            "valid": True,
            "test_functions": names,
            "count": len(names),
        }

    @staticmethod
    def _summary_counts(output: str) -> dict[str, int]:
        """Extract pytest outcome counts from the final summary line.

        Pytest may emit summaries such as:
        - ``7 passed in 0.10s``
        - ``5 passed, 1 skipped in 1.20s``
        - ``1 failed, 6 passed, 2 warnings in 2.00s``
        """
        counts = {
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "xfailed": 0,
            "xpassed": 0,
        }
        patterns = {
            "passed": r"(\d+)\s+passed",
            "failed": r"(\d+)\s+failed",
            "skipped": r"(\d+)\s+skipped",
            "errors": r"(\d+)\s+errors?",
            "xfailed": r"(\d+)\s+xfailed",
            "xpassed": r"(\d+)\s+xpassed",
        }
        for name, pattern in patterns.items():
            matches = re.findall(pattern, output, flags=re.IGNORECASE)
            if matches:
                counts[name] = int(matches[-1])
        return counts

    def run_pytest(
        self,
        relative_path: str = "tests/engineering_agent",
        coverage: bool = False,
    ) -> dict[str, object]:
        target = (self.repository_root / relative_path).resolve()
        allowed = [
            (self.repository_root / "tests").resolve(),
            self.generated_tests_root.resolve(),
        ]
        if not any(target.is_relative_to(base) for base in allowed):
            raise ValueError("pytest can only run tests/ or generated_tests/.")
        if not target.exists():
            raise FileNotFoundError(relative_path)

        command = [sys.executable, "-m", "pytest", str(target), "-q"]
        if coverage:
            command.extend(["--cov=app", "--cov-report=term-missing"])

        result = subprocess.run(
            command,
            cwd=self.repository_root,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        stdout = result.stdout[-30_000:]
        stderr = result.stderr[-10_000:]
        counts = self._summary_counts(f"{stdout}\n{stderr}")

        if result.returncode != 0:
            status = "failed"
        elif counts["passed"] == 0 and counts["skipped"] > 0:
            status = "skipped_only"
        elif counts["skipped"] > 0:
            status = "passed_with_skips"
        else:
            status = "passed"

        return {
            "command": " ".join(command),
            "path": relative_path,
            "exit_code": result.returncode,
            "status": status,
            "passed": counts["passed"],
            "failed": counts["failed"],
            "skipped": counts["skipped"],
            "errors": counts["errors"],
            "xfailed": counts["xfailed"],
            "xpassed": counts["xpassed"],
            "stdout": stdout,
            "stderr": stderr,
        }
