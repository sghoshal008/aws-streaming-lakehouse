from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    repository_root: Path
    generated_tests_root: Path
    api_base_url: str
    log_level: str
    aws_enabled: bool
    aws_region: str
    aws_profile: str | None
    llm_enabled: bool
    openai_model: str
    max_file_chars: int
    pytest_timeout_seconds: int


def load_settings() -> Settings:
    repository_root = Path(os.getenv("AGENT_REPOSITORY_ROOT", Path.cwd())).expanduser().resolve()
    generated_tests_root = (repository_root / "generated_tests").resolve()
    generated_tests_root.mkdir(parents=True, exist_ok=True)
    return Settings(
        repository_root=repository_root,
        generated_tests_root=generated_tests_root,
        api_base_url=os.getenv("AGENT_API_BASE_URL", "http://localhost:8000").rstrip("/"),
        log_level=os.getenv("AGENT_LOG_LEVEL", "INFO").upper(),
        aws_enabled=_as_bool(os.getenv("AGENT_ENABLE_AWS")),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
        aws_profile=os.getenv("AWS_PROFILE") or None,
        llm_enabled=_as_bool(os.getenv("AGENT_ENABLE_LLM")),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        max_file_chars=int(os.getenv("AGENT_MAX_FILE_CHARS", "60000")),
        pytest_timeout_seconds=int(os.getenv("AGENT_PYTEST_TIMEOUT_SECONDS", "180")),
    )
