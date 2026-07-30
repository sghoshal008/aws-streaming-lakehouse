from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from langchain.agents import create_agent
from langchain.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from app.engineering_agent.runtime import services
from app.engineering_agent.services.planning_service import build_test_plan, generate_scaffold


SYSTEM_PROMPT = """You are the Streaming Lakehouse Engineering Copilot, a practical senior data engineer.

Use tools whenever repository facts, test results, or AWS facts are needed. Choose the most
specific engineering tool available. Do not perform repository discovery when a direct tool
already represents the user's requested action.

Tool-selection rules:
1. "Run the Engineering Copilot tests" -> call run_engineering_copilot_tests immediately.
2. "Run generated tests" -> call run_generated_tests immediately.
3. Review/explain a selected Glue or Spark file -> call review_glue_or_spark_job first.
4. Generate tests for a selected Glue/Python file -> call propose_pytest_tests first.
5. Find or explain deduplication -> call find_deduplication_logic first.
6. Use low-level list/read/search tools only for follow-up evidence that a domain tool did not provide.
7. Do not call the same tool with the same arguments twice in one request.
8. Stop once you have enough evidence; avoid exploratory loops.

Accuracy and safety rules:
- Never invent file contents, test results, AWS results, or tool calls.
- Report passed, failed, skipped, and error counts exactly as returned by pytest.
- A skipped-only suite is not a passed test suite.
- Never modify production source files.
- Do not write generated files autonomously. You may propose code, but writing requires the
  separate approval endpoint in the UI.
- Run pytest only through approved test tools.
- Keep repository paths relative to the repository root.
- AWS tools are read-only and require explicit identifiers.
- Answer conversationally with findings, implications, and concrete next steps.
"""


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, default=str)


# ---------------------------------------------------------------------------
# High-level engineering tools: preferred by the ReAct agent
# ---------------------------------------------------------------------------


@tool
def review_glue_or_spark_job(path: str) -> str:
    """Review one Glue/PySpark Python file and return testability, Spark operations, functions, and concrete risks."""
    analysis = services().repository.analyse_python(path)
    source = services().repository.read_file(path)
    findings: list[str] = []
    if not analysis["import_safe"]:
        findings.append(
            "The module is not import-safe because Glue, Spark, or AWS runtime initialisation occurs at module scope."
        )
    if analysis["spark_operations"]:
        findings.append(
            "Detected Spark operations: " + ", ".join(analysis["spark_operations"])
        )
    if "broadcast(" in source or "F.broadcast(" in source:
        findings.append("A broadcast join is present and should be tested for matched and unmatched records.")
    if "dropDuplicates" in source or "row_number" in source or "Window." in source:
        findings.append("Deduplication/window logic is present and needs deterministic winner tests.")
    if "quarantine" in source.lower() or "dq_" in source.lower():
        findings.append("Data-quality or quarantine behaviour is present and should be tested separately.")

    return _json(
        {
            "path": path,
            "line_count": analysis["line_count"],
            "functions": analysis["functions"],
            "spark_operations": analysis["spark_operations"],
            "import_safe": analysis["import_safe"],
            "top_level_runtime_initialisation": analysis["top_level_runtime_initialisation"],
            "findings": findings,
            "recommended_refactor": (
                "Extract pure DataFrame-to-DataFrame transformations into an import-safe module and keep Glue/AWS bootstrap in the entry point."
                if not analysis["import_safe"]
                else "The file can be imported locally; unit-test its transformation functions with small DataFrames."
            ),
        }
    )


@tool
def propose_pytest_tests(path: str) -> str:
    """Generate executable, lightweight pytest source-contract tests for one selected Glue/Python file."""
    analysis = services().repository.analyse_python(path)
    plan = build_test_plan(path, analysis)
    filename, content = generate_scaffold(path, analysis, plan)
    validation = services().tests.validate_python(content)
    return _json(
        {
            "path": path,
            "analysis": analysis,
            "test_plan": plan,
            "proposed_filename": filename,
            "proposed_content": content,
            "validation": validation,
            "write_policy": "The proposal is not written automatically. Use the UI approval action to write it under generated_tests/.",
        }
    )


@tool
def find_deduplication_logic(path: str = "app/glue/bronze-to-silver/iata_sales_bronze_to_silver.py") -> str:
    """Find and explain deduplication logic in a specific file, including business keys and ordering expressions."""
    source = services().repository.read_file(path)
    lines = source.splitlines()
    keywords = ("dropDuplicates", "row_number", "Window.", "partitionBy", "orderBy", "dedup")
    matches = [
        {"line": number, "text": line.strip()}
        for number, line in enumerate(lines, 1)
        if any(keyword.lower() in line.lower() for keyword in keywords)
    ]
    return _json({"path": path, "matches": matches[:80], "match_count": len(matches)})


@tool
def run_engineering_copilot_tests(coverage: bool = False) -> str:
    """Run the Engineering Copilot's own unit tests under tests/engineering_agent and return exact outcome counts."""
    return _json(
        services().tests.run_pytest(
            relative_path="tests/engineering_agent",
            coverage=coverage,
        )
    )


@tool
def run_generated_tests(coverage: bool = False) -> str:
    """
    Run all human-approved tests under generated_tests.

    Always report the exact numeric values returned for:
    passed, failed, skipped, errors, and status.
    Do not replace the counts with a vague summary.
    """
    return _json(
        services().tests.run_pytest(
            relative_path="generated_tests",
            coverage=coverage,
        )
    )
    

@tool
def explain_pipeline_architecture() -> str:
    """Read repository documentation and summarize the streaming-lakehouse data flow and major AWS components."""
    candidates = [
        "README.md",
        "app/acquisition/README.md",
        "app/glue/landing-to-msk/README.md",
        "app/glue/bronze-to-silver/README.MD",
    ]
    documents: dict[str, str] = {}
    for candidate in candidates:
        try:
            documents[candidate] = services().repository.read_file(candidate)[:12_000]
        except FileNotFoundError:
            continue
    return _json({"documents": documents})


# ---------------------------------------------------------------------------
# Low-level repository tools: used only when more evidence is needed
# ---------------------------------------------------------------------------


@tool
def list_repository_files(pattern: str = "*.py", limit: int = 100) -> str:
    """List repository files matching a glob pattern. Use only when no domain-specific tool can answer directly."""
    return _json(services().repository.list_files(pattern=pattern, limit=limit))


@tool
def read_repository_file(path: str) -> str:
    """Read an approved text file using a repository-relative path."""
    return services().repository.read_file(path)


@tool
def search_repository(query: str, pattern: str = "*.py", limit: int = 30) -> str:
    """Search repository text and return matching paths, line numbers, and snippets."""
    return _json(services().repository.search(query=query, pattern=pattern, limit=limit))


@tool
def analyse_python_file(path: str) -> str:
    """Return raw AST analysis for one Python file. Prefer review_glue_or_spark_job for normal reviews."""
    return _json(services().repository.analyse_python(path))


@tool
def validate_python_test(content: str) -> str:
    """Validate proposed Python pytest code without writing or executing it."""
    return _json(services().tests.validate_python(content))


@tool
def run_pytest(path: str, coverage: bool = False) -> str:
    """Run an explicit approved path under tests/ or generated_tests/. Prefer the dedicated test-suite tools when applicable."""
    return _json(services().tests.run_pytest(relative_path=path, coverage=coverage))


# ---------------------------------------------------------------------------
# Optional read-only AWS investigation tools
# ---------------------------------------------------------------------------


@tool
def get_glue_job_run(job_name: str, run_id: str) -> str:
    """Read one explicit AWS Glue job run. Read-only AWS access must be enabled."""
    return _json(services().aws.get_glue_job_run(job_name, run_id))


@tool
def get_control_item(table_name: str, pk: str, sk: str) -> str:
    """Read one explicit DynamoDB control-table item by pk and sk. Read-only AWS access must be enabled."""
    return _json(services().aws.get_control_item(table_name, pk, sk))


@tool
def read_s3_text(bucket: str, key: str, max_bytes: int = 100_000) -> str:
    """Read a bounded UTF-8 text object from S3. Read-only AWS access must be enabled."""
    return services().aws.read_s3_text(bucket, key, max_bytes)


TOOLS = [
    review_glue_or_spark_job,
    propose_pytest_tests,
    find_deduplication_logic,
    run_engineering_copilot_tests,
    run_generated_tests,
    explain_pipeline_architecture,
    list_repository_files,
    read_repository_file,
    search_repository,
    analyse_python_file,
    validate_python_test,
    run_pytest,
    get_glue_job_run,
    get_control_item,
    read_s3_text,
]


@lru_cache(maxsize=1)
def build_react_agent():
    cfg = services().settings
    if not cfg.llm_enabled:
        raise RuntimeError(
            "The ReAct agent is disabled. Set AGENT_ENABLE_LLM=true and OPENAI_API_KEY, then restart FastAPI."
        )
    return create_agent(
        model=f"openai:{cfg.openai_model}",
        tools=TOOLS,
        system_prompt=SYSTEM_PROMPT,
        checkpointer=InMemorySaver(),
        name="streaming_lakehouse_engineering_copilot",
    )


def _message_text(message: Any) -> str:
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") in {"text", "output_text"}:
                parts.append(str(item.get("text", "")))
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(part for part in parts if part)
    return str(content)


async def invoke_react_agent(
    *,
    message: str,
    thread_id: str,
    selected_file: str | None = None,
) -> dict[str, Any]:
    agent = build_react_agent()
    context = message.strip()
    if selected_file:
        context += (
            "\n\nSelected repository file: "
            f"{selected_file}. Use it when the request refers to the selected job or file."
        )

    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": context}]},
        config={"configurable": {"thread_id": thread_id}, "recursion_limit": 12},
    )

    messages = result.get("messages", [])
    answer = _message_text(messages[-1]) if messages else "The agent returned no response."
    tool_messages = [item for item in messages if item.__class__.__name__ == "ToolMessage"]
    tool_trace = [str(getattr(item, "name", "tool")) for item in tool_messages]

    proposal: dict[str, Any] = {}
    for item in tool_messages:
        if getattr(item, "name", "") != "propose_pytest_tests":
            continue
        try:
            payload = json.loads(_message_text(item))
        except (TypeError, json.JSONDecodeError):
            continue
        if payload.get("proposed_filename") and payload.get("proposed_content"):
            proposal = {
                "test_plan": payload.get("test_plan", []),
                "proposed_filename": payload["proposed_filename"],
                "proposed_content": payload["proposed_content"],
                "analysis": payload.get("analysis"),
            }

    session_value = {
        "last_answer": answer,
        "tool_trace": tool_trace,
        "selected_file": selected_file,
        **proposal,
    }
    services().sessions.put(thread_id, session_value)
    return {"answer": answer, "tool_trace": tool_trace, **proposal}
