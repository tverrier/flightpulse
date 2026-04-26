"""Dashboard query tests using a fake Snowflake DBAPI connection.

We don't need a live Snowflake — just verify the SQL we build, the params we
bind, and the shape of the DataFrame we return. Each fake cursor records
the (sql, params) pair so we can assert against it.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Sequence

import pytest

pd = pytest.importorskip("pandas")

from serving.dashboard import queries  # noqa: E402


class _FakeCursor:
    def __init__(self, rows: list[Sequence[Any]], columns: list[str]) -> None:
        self._rows = rows
        self._columns = columns
        self.description: list[tuple[str, ...]] = [(c,) for c in columns]
        self.last_sql: str | None = None
        self.last_params: Sequence[Any] | None = None

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self.last_sql = sql
        self.last_params = tuple(params or ())

    def fetchall(self) -> list[Sequence[Any]]:
        return self._rows

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        pass


def _make_conn(rows: list[tuple[Any, ...]], columns: list[str]) -> tuple[_FakeConn, _FakeCursor]:
    cur = _FakeCursor(rows, columns)
    return _FakeConn(cur), cur


# -----------------------------------------------------------------------------
# fetch_carrier_options
# -----------------------------------------------------------------------------
def test_fetch_carrier_options_returns_dataframe():
    conn, cur = _make_conn(
        rows=[("WN", "Southwest"), ("DL", "Delta")],
        columns=["CARRIER_KEY", "CARRIER_NAME"],
    )
    df = queries.fetch_carrier_options(conn)
    assert list(df.columns) == ["carrier_key", "carrier_name"]
    assert len(df) == 2
    assert "FLIGHTPULSE_PROD.MARTS.dim_carrier" in (cur.last_sql or "")


# -----------------------------------------------------------------------------
# fetch_otp_overview
# -----------------------------------------------------------------------------
def test_fetch_otp_overview_filters_by_window_and_carriers():
    conn, cur = _make_conn(
        rows=[(dt.date(2026, 4, 20), "WN", "Southwest", 100, 95, 5, 88.0, 6.5, 800)],
        columns=[
            "FLIGHT_DATE", "CARRIER_KEY", "CARRIER_NAME",
            "FLIGHTS_SCHEDULED", "FLIGHTS_COMPLETED", "FLIGHTS_CANCELLED",
            "ON_TIME_PCT", "AVG_ARR_DELAY_MIN", "TOTAL_DELAY_MIN",
        ],
    )
    df = queries.fetch_otp_overview(
        conn,
        start_date=dt.date(2026, 4, 1),
        end_date=dt.date(2026, 4, 25),
        carriers=["WN", "DL"],
    )
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["carrier_key"] == "WN"
    assert cur.last_params == (dt.date(2026, 4, 1), dt.date(2026, 4, 25), "WN", "DL")
    assert "FLIGHTPULSE_PROD.MARTS.agg_carrier_otp_daily" in (cur.last_sql or "")
    assert "carrier_key in (%s,%s)" in (cur.last_sql or "")


def test_fetch_otp_overview_omits_carrier_filter_when_none():
    conn, cur = _make_conn(rows=[], columns=["FLIGHT_DATE"])
    queries.fetch_otp_overview(
        conn,
        start_date=dt.date(2026, 4, 1),
        end_date=dt.date(2026, 4, 25),
        carriers=None,
    )
    assert cur.last_params == (dt.date(2026, 4, 1), dt.date(2026, 4, 25))
    assert "carrier_key in" not in (cur.last_sql or "")


# -----------------------------------------------------------------------------
# fetch_top_delay_routes
# -----------------------------------------------------------------------------
def test_fetch_top_delay_routes_passes_limit():
    conn, cur = _make_conn(
        rows=[("DEN", "LAX", 200, 18.5, 60, 70.0)],
        columns=["ORIGIN_IATA", "DEST_IATA", "FLIGHTS", "AVG_ARR_DELAY_MIN", "FLIGHTS_LATE", "ON_TIME_PCT"],
    )
    df = queries.fetch_top_delay_routes(
        conn,
        start_date=dt.date(2026, 4, 1),
        end_date=dt.date(2026, 4, 25),
        limit=10,
    )
    assert df.iloc[0]["origin_iata"] == "DEN"
    assert cur.last_params == (dt.date(2026, 4, 1), dt.date(2026, 4, 25), 10)
    assert "fct_flight_event" in (cur.last_sql or "")


# -----------------------------------------------------------------------------
# fetch_recent_flights
# -----------------------------------------------------------------------------
def test_fetch_recent_flights_filters_carrier_and_date():
    conn, cur = _make_conn(rows=[], columns=["FLIGHT_DATE"])
    queries.fetch_recent_flights(
        conn,
        flight_date=dt.date(2026, 4, 20),
        carrier="WN",
        limit=50,
    )
    assert cur.last_params == (dt.date(2026, 4, 20), "WN", 50)


# -----------------------------------------------------------------------------
# fetch_kpi_summary
# -----------------------------------------------------------------------------
def test_fetch_kpi_summary_returns_dict():
    conn, _ = _make_conn(
        rows=[(1000, 950, 50, 87.5, 12345)],
        columns=[
            "FLIGHTS_SCHEDULED", "FLIGHTS_COMPLETED", "FLIGHTS_CANCELLED",
            "ON_TIME_PCT", "TOTAL_DELAY_MIN",
        ],
    )
    out = queries.fetch_kpi_summary(
        conn,
        start_date=dt.date(2026, 4, 1),
        end_date=dt.date(2026, 4, 25),
    )
    assert out["flights_scheduled"] == 1000.0
    assert out["on_time_pct"] == 87.5
    assert out["total_delay_min"] == 12345.0


def test_fetch_kpi_summary_empty_returns_zeros():
    conn, _ = _make_conn(rows=[], columns=["FLIGHTS_SCHEDULED"])
    out = queries.fetch_kpi_summary(
        conn,
        start_date=dt.date(2026, 4, 1),
        end_date=dt.date(2026, 4, 25),
    )
    assert out["flights_scheduled"] == 0.0
    assert out["on_time_pct"] == 0.0
