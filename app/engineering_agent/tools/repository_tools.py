from __future__ import annotations
from app.engineering_agent.runtime import services


def list_repository_files(pattern: str = "*.py", limit: int = 200): return services().repository.list_files(pattern, limit)
def read_repository_file(path: str): return services().repository.read_file(path)
def search_repository(query: str, pattern: str = "*.py", limit: int = 50): return services().repository.search(query, pattern, limit)
def analyse_python_file(path: str): return services().repository.analyse_python(path)
