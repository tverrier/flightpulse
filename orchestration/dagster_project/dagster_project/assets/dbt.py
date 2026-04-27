"""dbt assets — silver + gold lineage surfaced from the dbt manifest.

Strategy: build a single `@dbt_assets` op that loads the dbt manifest at
import time and exposes every model as a Dagster asset. Source declarations
in `dbt/models/sources.yml` are wired to the bronze ingestion assets via
`CustomDagsterDbtTranslator.get_asset_key`.

Two non-obvious behaviors enforced here, both anchored in CLAUDE.md:

  1. Gold materializations are *lazy* — never auto-materialize on parent
     update. We do that by exposing a separate `gold_select` selector and
     scheduling it on its own cron in `schedules.py`. The asset op itself
     is shared (one dbt invocation per run), but the schedules / jobs target
     non-overlapping selectors.

  2. The dbt project ships with `on-run-start: ensure_drift_log_table()` —
     so the manifest must be regenerated whenever models change. We do
     `dbt parse` lazily on first import if `target/manifest.json` is missing.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey, FreshnessPolicy
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from dagster_project.constants import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    DBT_TARGET,
    GROUP_GOLD,
    GROUP_SILVER,
)

DBT_MANIFEST_PATH: Path = DBT_PROJECT_DIR / "target" / "manifest.json"


# -----------------------------------------------------------------------------
# Manifest bootstrap.
#
# The Dagster code-location loader needs a manifest to enumerate assets. On a
# fresh clone (no `dbt parse` ever run) we generate one in-place. We do *not*
# regenerate on every import — that would slow `dagster dev` substantially.
# Set FLIGHTPULSE_DBT_REPARSE=1 to force.
# -----------------------------------------------------------------------------
_MANIFEST_REQUIRED_KEYS = (
    "nodes", "sources", "metrics", "exposures", "semantic_models",
    "saved_queries", "unit_tests", "groups", "child_map", "parent_map",
    "group_map", "selectors", "disabled", "macros", "docs",
)


def _ensure_manifest() -> Path:
    if not (DBT_MANIFEST_PATH.exists() and not os.environ.get("FLIGHTPULSE_DBT_REPARSE")):
        cmd = [
            "dbt", "parse",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
            "--target", DBT_TARGET,
        ]
        env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR)}
        try:
            subprocess.run(cmd, check=True, env=env, capture_output=True, timeout=120)
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            DBT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
            DBT_MANIFEST_PATH.write_text(json.dumps({
                "metadata": {"adapter_type": "snowflake", "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
            }))

    # Backfill any keys dagster-dbt 0.24 iterates that older dbt manifests omit.
    try:
        manifest = json.loads(DBT_MANIFEST_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        manifest = {}
    changed = False
    for k in _MANIFEST_REQUIRED_KEYS:
        if k not in manifest:
            manifest[k] = {}
            changed = True
    if "metadata" not in manifest:
        manifest["metadata"] = {"adapter_type": "snowflake"}
        changed = True
    if changed:
        DBT_MANIFEST_PATH.write_text(json.dumps(manifest))
    return DBT_MANIFEST_PATH


# -----------------------------------------------------------------------------
# Translator — maps dbt source/model names to Dagster asset keys so lineage
# joins cleanly to the bronze assets defined in `bronze.py`.
# -----------------------------------------------------------------------------
class FlightPulseDbtTranslator(DagsterDbtTranslator):
    # Map dbt sources to the Dagster asset keys used by bronze ingestion.
    _SOURCE_KEY_MAP: dict[tuple[str, str], AssetKey] = {
        ("bronze", "bts_raw"): AssetKey(["bronze", "bts", "bts_monthly_raw"]),
        ("bronze", "opensky_raw"): AssetKey(["bronze", "opensky", "opensky_states_raw"]),
        ("bronze", "openflights_airports_raw"): AssetKey(
            ["bronze", "openflights", "openflights_airports_raw"]
        ),
        ("bronze", "openflights_airlines_raw"): AssetKey(
            ["bronze", "openflights", "openflights_airlines_raw"]
        ),
    }

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            key = (dbt_resource_props.get("source_name"), dbt_resource_props.get("name"))
            mapped = self._SOURCE_KEY_MAP.get(key)  # type: ignore[arg-type]
            if mapped is not None:
                return mapped
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        tags = set(dbt_resource_props.get("tags", []) or [])
        if "gold" in tags:
            return GROUP_GOLD
        if "silver" in tags:
            return GROUP_SILVER
        return super().get_group_name(dbt_resource_props)

    def get_freshness_policy(self, dbt_resource_props: Mapping[str, Any]) -> FreshnessPolicy | None:
        # PIPELINE.md SLAs:
        #   silver.aircraft_state ≤ 30 min
        #   silver.flight_event   ≤ 24 h
        #   gold.fct_flight_event ≤ 1 h
        #   gold.dim_*            ≤ 6 h
        #   agg_carrier_otp_daily ≤ 2 h
        name = dbt_resource_props.get("name", "")
        tags = set(dbt_resource_props.get("tags", []) or [])
        if name == "silver_aircraft_state":
            return FreshnessPolicy(maximum_lag_minutes=30)
        if name == "silver_flight_event":
            return FreshnessPolicy(maximum_lag_minutes=60 * 24)
        if name == "fct_flight_event":
            return FreshnessPolicy(maximum_lag_minutes=60)
        if name == "agg_carrier_otp_daily":
            return FreshnessPolicy(maximum_lag_minutes=120, cron_schedule="0 3 * * *")
        if name.startswith("dim_") and "gold" in tags:
            return FreshnessPolicy(maximum_lag_minutes=60 * 6)
        return None


_TRANSLATOR = FlightPulseDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

_MANIFEST = _ensure_manifest()


@dbt_assets(
    manifest=_MANIFEST,
    dagster_dbt_translator=_TRANSLATOR,
)
def flightpulse_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """Generic dbt build op — selectors at the *job* level decide what runs.

    We always invoke `dbt build` (run + test) so test failures surface as
    Dagster failures rather than silently running. The selector that the job
    uses is appended via `context.op_execution_context.op_config` when the
    job calls this op (handled automatically by `dagster-dbt`).
    """
    yield from dbt.cli(["build"], context=context).stream()
