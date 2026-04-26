"""Top-level pytest fixtures.

Adds the repo root and the Dagster project's package root to sys.path so
tests can `from dagster_project import defs` without requiring the package
to be pip-installed in the venv.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DAGSTER_PKG_ROOT = _REPO_ROOT / "orchestration" / "dagster_project"

for p in (_REPO_ROOT, _DAGSTER_PKG_ROOT):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
