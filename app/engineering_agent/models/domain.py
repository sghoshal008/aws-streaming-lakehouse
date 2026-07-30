from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ToolEvent:
    tool: str
    status: str = "completed"
    detail: str | None = None


@dataclass
class TestProposal:
    filename: str
    content: str
    scenarios: list[dict[str, str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
