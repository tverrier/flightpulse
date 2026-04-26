"""Asset registry for the FlightPulse Dagster project.

Re-exports the symbols used by `definitions.py` so we have a single import
surface and a stable dotted path for asset selection.
"""

from dagster_project.assets.bronze import (
    bts_monthly_raw,
    openflights_airlines_raw,
    openflights_airports_raw,
    opensky_states_raw,
)
from dagster_project.assets.dbt import (
    DBT_MANIFEST_PATH,
    flightpulse_dbt_assets,
)

__all__ = [
    "bts_monthly_raw",
    "openflights_airports_raw",
    "openflights_airlines_raw",
    "opensky_states_raw",
    "flightpulse_dbt_assets",
    "DBT_MANIFEST_PATH",
]
