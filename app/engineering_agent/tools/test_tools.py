from __future__ import annotations

from app.engineering_agent.runtime import services
from app.engineering_agent.services.planning_service import build_test_plan, generate_scaffold


def propose_generated_test(path: str):
    """Create an executable pytest proposal for a repository file without writing it."""
    analysis = services().repository.analyse_python(path)
    plan = build_test_plan(path, analysis)
    filename, content = generate_scaffold(path, analysis, plan)
    return {
        "path": path,
        "analysis": analysis,
        "test_plan": plan,
        "proposed_filename": filename,
        "proposed_content": content,
        "validation": services().tests.validate_python(content),
        "requires_approval": True,
    }


def validate_generated_test(content: str):
    return services().tests.validate_python(content)


def write_generated_test(filename: str, content: str, approved: bool = False):
    if not approved:
        raise PermissionError("Explicit approval is required.")
    return {"written": True, "path": services().repository.write_generated_test(filename, content)}


def run_pytest(path: str = "tests/engineering_agent", coverage: bool = False):
    return services().tests.run_pytest(path, coverage)
