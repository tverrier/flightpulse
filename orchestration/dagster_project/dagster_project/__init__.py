"""FlightPulse Dagster code location.

The Dagster CLI looks for a top-level `defs` symbol; we re-export the
`Definitions` object built in `definitions.py` so `dagster dev -m
dagster_project` resolves it without any additional flags.
"""

from dagster_project.definitions import defs

__all__ = ["defs"]
