"""Asset-level tests that don't need real AWS / dbt running."""

from __future__ import annotations

import datetime as dt
from typing import Any
from unittest import mock

import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_aws")
pytest.importorskip("dagster_dbt")

from dagster import AssetKey, build_op_context  # noqa: E402

from dagster_project.assets.bronze import (  # noqa: E402
    bts_monthly_raw,
    openflights_airports_raw,
    opensky_states_raw,
)


# -----------------------------------------------------------------------------
# bts_monthly_raw delegates to ingestion.airbyte.run_bts_sync.main
# -----------------------------------------------------------------------------
def test_bts_monthly_raw_invokes_runner_with_partition_month(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_main():
        import sys
        captured["argv"] = list(sys.argv)
        return 0

    # Patch at the source module — the asset imports it lazily.
    monkeypatch.setenv("S3_RAW_BUCKET", "test-bucket")
    monkeypatch.setattr(
        "ingestion.airbyte.run_bts_sync.main", fake_main, raising=True
    )

    ctx = build_op_context(partition_key="2024-01-01")
    result = bts_monthly_raw(ctx)

    assert result.metadata["month"].text == "2024-01"
    assert "test-bucket" in result.metadata["s3_uri"].path
    assert "--month" in captured["argv"]
    assert "2024-01" in captured["argv"]
    assert "--bucket" in captured["argv"]
    assert "test-bucket" in captured["argv"]


def test_bts_monthly_raw_propagates_failure(monkeypatch):
    monkeypatch.setattr(
        "ingestion.airbyte.run_bts_sync.main", lambda: 4, raising=True
    )
    ctx = build_op_context(partition_key="2024-02-01")
    with pytest.raises(RuntimeError, match="rc=4"):
        bts_monthly_raw(ctx)


# -----------------------------------------------------------------------------
# OpenFlights asset uses runner with explicit argv (not sys.argv).
# -----------------------------------------------------------------------------
def test_openflights_airports_calls_runner(monkeypatch):
    seen_argv: dict[str, Any] = {}

    def fake_main(argv):
        seen_argv["argv"] = list(argv)
        return 0

    monkeypatch.setenv("S3_RAW_BUCKET", "test-bucket")
    monkeypatch.setattr(
        "ingestion.seeds.load_openflights.main", fake_main, raising=True
    )

    ctx = build_op_context()
    result = openflights_airports_raw(ctx)

    argv = seen_argv["argv"]
    assert argv[argv.index("--bucket") + 1] == "test-bucket"
    assert "openflights" in argv
    assert "airports.csv" in result.metadata["s3_uri"].path


# -----------------------------------------------------------------------------
# opensky_states_raw is an observable_source_asset → returns a DataVersion.
# -----------------------------------------------------------------------------
class _FakeS3Resource:
    """Stand-in for dagster_aws.s3.S3Resource that returns canned objects."""

    def __init__(self, objects: list[dict[str, Any]]):
        self._objects = objects

    def get_client(self):  # noqa: ANN201
        outer = self

        class _FakePaginator:
            def paginate(self, **_kwargs):
                yield {"Contents": outer._objects}

        class _FakeClient:
            def get_paginator(self, _name):
                return _FakePaginator()

        return _FakeClient()


def test_opensky_states_raw_returns_latest_mtime():
    now = dt.datetime.now(dt.timezone.utc)
    objs = [
        {"Key": "opensky/ds=2026-04-25/hr=10/00-x.parquet", "LastModified": now - dt.timedelta(seconds=90)},
        {"Key": "opensky/ds=2026-04-25/hr=10/01-y.parquet", "LastModified": now - dt.timedelta(seconds=30)},
    ]
    s3 = _FakeS3Resource(objs)

    ctx = build_op_context()
    dv = opensky_states_raw(ctx, s3=s3)  # type: ignore[arg-type]
    expected = max(o["LastModified"] for o in objs).isoformat()
    assert dv.value == expected


def test_opensky_states_raw_handles_empty_bucket():
    ctx = build_op_context()
    dv = opensky_states_raw(ctx, s3=_FakeS3Resource([]))  # type: ignore[arg-type]
    assert dv.value == "empty"


# -----------------------------------------------------------------------------
# Lineage: dbt assets must declare bronze keys as upstream.
# -----------------------------------------------------------------------------
def test_dbt_translator_maps_bronze_sources():
    from dagster_project.assets.dbt import FlightPulseDbtTranslator

    t = FlightPulseDbtTranslator()
    bts_props = {"resource_type": "source", "source_name": "bronze", "name": "bts_raw"}
    assert t.get_asset_key(bts_props) == AssetKey(["bronze", "bts", "bts_monthly_raw"])

    opensky_props = {"resource_type": "source", "source_name": "bronze", "name": "opensky_raw"}
    assert t.get_asset_key(opensky_props) == AssetKey(["bronze", "opensky", "opensky_states_raw"])
