from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.engineering_agent.agent.react_agent import invoke_react_agent
from app.engineering_agent.mcp.server import mcp
from app.engineering_agent.models.api import AgentRequest, AgentResponse, ApprovalRequest, TestRunRequest
from app.engineering_agent.runtime import services
from app.engineering_agent.utils.logging import configure_logging

configure_logging(services().settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(
    title="Streaming Lakehouse Engineering Copilot",
    version="4.0.0-simple-react-mcp",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Mcp-Session-Id"],
)


@app.get("/health")
def health() -> dict[str, object]:
    cfg = services().settings
    return {
        "status": "healthy",
        "version": "4.0.0-simple-react-mcp",
        "agent_type": "OpenAI ReAct agent with domain engineering tools",
        "aws_enabled": cfg.aws_enabled,
        "llm_enabled": cfg.llm_enabled,
        "openai_model": cfg.openai_model,
        "repository_root": str(cfg.repository_root),
    }


@app.post("/api/agent/invoke", response_model=AgentResponse)
async def invoke_agent(payload: AgentRequest) -> AgentResponse:
    try:
        result = await invoke_react_agent(
            message=payload.message,
            thread_id=payload.thread_id,
            selected_file=payload.selected_file,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Agent execution failed: {exc}") from exc

    return AgentResponse(
        thread_id=payload.thread_id,
        answer=result["answer"],
        action="react_agent",
        analysis=result.get("analysis"),
        test_plan=result.get("test_plan", []),
        proposed_filename=result.get("proposed_filename"),
        proposed_content=result.get("proposed_content"),
        tool_trace=result.get("tool_trace", []),
    )


@app.get("/api/sessions/{thread_id}")
def get_session(thread_id: str) -> dict[str, object]:
    return services().sessions.get(thread_id)


@app.post("/api/generated-tests/approve")
def approve_generated_test(payload: ApprovalRequest) -> dict[str, object]:
    if not payload.approved:
        raise HTTPException(400, "Explicit approval is required.")
    try:
        path = services().repository.write_generated_test(payload.filename, payload.content)
    except (ValueError, SyntaxError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"written": True, "path": path}


@app.post("/api/tests/run")
def run_tests(payload: TestRunRequest) -> dict[str, object]:
    try:
        return services().tests.run_pytest(payload.path)
    except (ValueError, TimeoutError) as exc:
        raise HTTPException(400, str(exc)) from exc


app.mount("/mcp", mcp.streamable_http_app())
