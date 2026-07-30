from __future__ import annotations

from pathlib import Path


def resolve_within(root: Path, relative_path: str) -> Path:
    candidate = (root / relative_path).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("Path is outside the approved root.")
    return candidate
