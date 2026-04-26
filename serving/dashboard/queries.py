"""Snowflake query layer for the Streamlit dashboard.

Read-only access through the role `RL_DASHBOARD_RO` (per ARCHITECTURE.md
§Data Flow step 4). All queries:

  * are parameterized — no string concatenation of user input,
  * fully qualify objects to FLIGHTPULSE_PROD.MARTS so a misconfigured
    `SNOWFLAKE_DATABASE` env var fails loudly instead of silently reading
    flightpulse_dev,
  * return pandas DataFrames so Streamlit can chart them directly.

The connection factory is split out as `connect()` so tests can monkeypatch
it. Each query takes an explicit connection — no module-level singleton —
so Streamlit's `cache_resource` owns the lifecycle.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Protocol, Sequence

import pandas as pd

LOGGER = logging.getLogger("flightpulse.dashboard.queries")

GOLD_DATABASE = "FLIGHTPULSE_PROD"
GOLD_SCHEMA = "MARTS"
DASHBOARD_ROLE = "RL_DASHBOARD_RO"
DASHBOARD_WAREHOUSE_DEFAULT = "WH_FLIGHTPULSE_XS"


class _CursorLike(Protocol):
    description: Sequence[Any]
    def execute(self, sql: str, params: Sequence[Any] | None = ...) -> Any: ...
    def fetchall(self) -> list[Sequence[Any]]: ...
    def close(self) -> None: ...


class _ConnectionLike(Protocol):
    def cursor(self) -> _CursorLike: ...
    def close(self) -> None: ...


def connect() -> _ConnectionLike:
    """Build a Snowflake connection from env vars in dashboard-RO mode.

    SETUP.md §8 documents the env-var contract; we override `role` to
    `RL_DASHBOARD_RO` so the dashboard can never accidentally write — the
    transformer role lives in the same env file but must not be used here.
    """
    import snowflake.connector  # type: ignore[import-not-found]

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_DASHBOARD_ROLE", DASHBOARD_ROLE),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", DASHBOARD_WAREHOUSE_DEFAULT),
        database=os.environ.get("SNOWFLAKE_DATABASE", GOLD_DATABASE),
        schema=GOLD_SCHEMA,
        client_session_keep_alive=True,
    )


def _fetch_df(conn: _ConnectionLike, sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description] if cur.description else []
    finally:
        cur.close()
    return pd.DataFrame(rows, columns=cols)


# -----------------------------------------------------------------------------
# Queries — each returns a pandas DataFrame with predictable column names.
# -----------------------------------------------------------------------------
def fetch_carrier_options(conn: _ConnectionLike) -> pd.DataFrame:
    """All carriers with at least one flight in the last 90 days."""
    sql = f"""
        select c.carrier_key, c.carrier_name
        from {GOLD_DATABASE}.{GOLD_SCHEMA}.dim_carrier c
        where exists (
            select 1
            from {GOLD_DATABASE}.{GOLD_SCHEMA}.agg_carrier_otp_daily a
            where a.carrier_key = c.carrier_key
              and a.flight_date >= dateadd(day, -90, current_date)
        )
        order by c.carrier_name
    """
    return _fetch_df(conn, sql)


def fetch_otp_overview(
    conn: _ConnectionLike,
    start_date: dt.date,
    end_date: dt.date,
    carriers: Sequence[str] | None = None,
) -> pd.DataFrame:
    """One row per (flight_date, carrier_key) over the requested window."""
    params: list[Any] = [start_date, end_date]
    where_carrier = ""
    if carriers:
        placeholders = ",".join(["%s"] * len(carriers))
        where_carrier = f"and a.carrier_key in ({placeholders})"
        params.extend(carriers)
    sql = f"""
        select
            a.flight_date,
            a.carrier_key,
            c.carrier_name,
            a.flights_scheduled,
            a.flights_completed,
            a.flights_cancelled,
            a.on_time_pct,
            a.avg_arr_delay_min,
            a.total_delay_min
        from {GOLD_DATABASE}.{GOLD_SCHEMA}.agg_carrier_otp_daily a
        join {GOLD_DATABASE}.{GOLD_SCHEMA}.dim_carrier c
          on c.carrier_key = a.carrier_key
        where a.flight_date between %s and %s
          {where_carrier}
        order by a.flight_date, a.carrier_key
    """
    return _fetch_df(conn, sql, params)


def fetch_top_delay_routes(
    conn: _ConnectionLike,
    start_date: dt.date,
    end_date: dt.date,
    limit: int = 25,
) -> pd.DataFrame:
    """Top-N (origin, dest) pairs by mean arrival delay. Cancelled flights excluded."""
    sql = f"""
        select
            f.origin_airport_key as origin_iata,
            f.dest_airport_key   as dest_iata,
            count(*)             as flights,
            avg(f.arr_delay_min) as avg_arr_delay_min,
            sum(case when f.arr_delay_min > 14 then 1 else 0 end) as flights_late,
            100.0 * sum(case when f.arr_delay_min <= 14 then 1 else 0 end)
                / nullif(count(*), 0) as on_time_pct
        from {GOLD_DATABASE}.{GOLD_SCHEMA}.fct_flight_event f
        where f.flight_date_key between %s and %s
          and not f.cancelled_flag
        group by 1, 2
        having count(*) >= 30
        order by avg_arr_delay_min desc nulls last
        limit %s
    """
    return _fetch_df(conn, sql, (start_date, end_date, limit))


def fetch_recent_flights(
    conn: _ConnectionLike,
    flight_date: dt.date,
    carrier: str,
    limit: int = 200,
) -> pd.DataFrame:
    """Single-day, single-carrier sample of fct_flight_event rows."""
    sql = f"""
        select
            f.flight_date_key       as flight_date,
            f.carrier_key,
            f.flight_number,
            f.origin_airport_key    as origin_iata,
            f.dest_airport_key      as dest_iata,
            f.scheduled_dep_ts,
            f.actual_dep_ts,
            f.scheduled_arr_ts,
            f.actual_arr_ts,
            f.dep_delay_min,
            f.arr_delay_min,
            f.cancelled_flag,
            f.cancellation_code,
            f.diverted_flag,
            f.distance_miles,
            f.reconciliation_status
        from {GOLD_DATABASE}.{GOLD_SCHEMA}.fct_flight_event f
        where f.flight_date_key = %s
          and f.carrier_key     = %s
        order by f.scheduled_dep_ts
        limit %s
    """
    return _fetch_df(conn, sql, (flight_date, carrier, limit))


def fetch_kpi_summary(
    conn: _ConnectionLike,
    start_date: dt.date,
    end_date: dt.date,
) -> dict[str, float]:
    """Single-row KPI totals for the header tiles."""
    sql = f"""
        select
            sum(flights_scheduled)               as flights_scheduled,
            sum(flights_completed)               as flights_completed,
            sum(flights_cancelled)               as flights_cancelled,
            avg(on_time_pct)                     as on_time_pct,
            sum(coalesce(total_delay_min, 0))    as total_delay_min
        from {GOLD_DATABASE}.{GOLD_SCHEMA}.agg_carrier_otp_daily
        where flight_date between %s and %s
    """
    df = _fetch_df(conn, sql, (start_date, end_date))
    if df.empty:
        return {
            "flights_scheduled": 0.0,
            "flights_completed": 0.0,
            "flights_cancelled": 0.0,
            "on_time_pct": 0.0,
            "total_delay_min": 0.0,
        }
    row = df.iloc[0].to_dict()
    return {k: float(v) if v is not None else 0.0 for k, v in row.items()}
