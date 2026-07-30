from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class AgentRequest(BaseModel):
    message: str = Field(min_length=1, max_length=5000)
    thread_id: str = Field(default="default", min_length=1, max_length=120)
    selected_file: str | None = None
    mode: Literal["auto", "repository", "spark_testing", "pipeline"] = "auto"


class AgentResponse(BaseModel):
    thread_id: str
    answer: str
    action: str
    analysis: dict[str, Any] | None = None
    test_plan: list[dict[str, str]] = Field(default_factory=list)
    proposed_filename: str | None = None
    proposed_content: str | None = None
    result: dict[str, Any] | None = None
    tool_trace: list[str] = Field(default_factory=list)


class ApprovalRequest(BaseModel):
    thread_id: str = "default"
    filename: str
    content: str
    approved: bool


class TestRunRequest(BaseModel):
    path: str = "generated_tests"
