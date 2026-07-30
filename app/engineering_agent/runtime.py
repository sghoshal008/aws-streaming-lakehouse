from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from app.engineering_agent.config import Settings, load_settings
from app.engineering_agent.services.aws_service import AwsReadService
from app.engineering_agent.services.repository_service import RepositoryService
from app.engineering_agent.services.test_service import PytestService
from app.engineering_agent.storage.session_store import InMemorySessionStore


@dataclass
class ServiceContainer:
    settings: Settings
    repository: RepositoryService
    tests: PytestService
    aws: AwsReadService
    sessions: InMemorySessionStore


@lru_cache(maxsize=1)
def services() -> ServiceContainer:
    cfg = load_settings()
    return ServiceContainer(
        settings=cfg,
        repository=RepositoryService(cfg.repository_root, cfg.generated_tests_root, cfg.max_file_chars),
        tests=PytestService(cfg.repository_root, cfg.generated_tests_root, cfg.pytest_timeout_seconds),
        aws=AwsReadService(cfg.aws_enabled, cfg.aws_region, cfg.aws_profile),
        sessions=InMemorySessionStore(),
    )
