"""Streamlit FlightPulse dashboard.

`make dashboard` runs this on :8501. Reads `FLIGHTPULSE_PROD.MARTS` through
the `RL_DASHBOARD_RO` role (see SETUP.md §4) — the dashboard process must
not be able to write to gold tables.

Layout:
  1. Sidebar: date range + carrier multiselect
  2. Header KPI tiles (totals over the window)
  3. Daily on-time % line chart by carrier
  4. Top-N high-delay routes table
  5. Per-carrier sample of recent flights
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
import streamlit as st

from serving.dashboard import queries as q

LOGGER = logging.getLogger("flightpulse.dashboard")

st.set_page_config(
    page_title="FlightPulse — On-Time Performance",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner=False)
def _get_connection():
    return q.connect()


@st.cache_data(ttl=300, show_spinner=False)
def _carriers() -> pd.DataFrame:
    return q.fetch_carrier_options(_get_connection())


@st.cache_data(ttl=300, show_spinner=False)
def _kpis(start: dt.date, end: dt.date) -> dict[str, float]:
    return q.fetch_kpi_summary(_get_connection(), start, end)


@st.cache_data(ttl=300, show_spinner=False)
def _otp(start: dt.date, end: dt.date, carriers: tuple[str, ...]) -> pd.DataFrame:
    return q.fetch_otp_overview(_get_connection(), start, end, list(carriers) or None)


@st.cache_data(ttl=300, show_spinner=False)
def _routes(start: dt.date, end: dt.date, limit: int) -> pd.DataFrame:
    return q.fetch_top_delay_routes(_get_connection(), start, end, limit)


@st.cache_data(ttl=120, show_spinner=False)
def _recent(flight_date: dt.date, carrier: str, limit: int) -> pd.DataFrame:
    return q.fetch_recent_flights(_get_connection(), flight_date, carrier, limit)


# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
st.sidebar.title("FlightPulse")
st.sidebar.caption("Reads FLIGHTPULSE_PROD.MARTS via RL_DASHBOARD_RO")

today = dt.date.today()
default_start = today - dt.timedelta(days=30)
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, today),
    max_value=today,
)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, today

try:
    carrier_df = _carriers()
except Exception as exc:  # pragma: no cover - shown in UI
    st.sidebar.error(f"Could not load carriers from Snowflake: {exc}")
    st.stop()

carrier_labels = {
    row.carrier_key: f"{row.carrier_key} — {row.carrier_name}"
    for row in carrier_df.itertuples(index=False)
}
selected_carriers = st.sidebar.multiselect(
    "Carriers",
    options=list(carrier_labels.keys()),
    default=list(carrier_labels.keys())[:5],
    format_func=lambda k: carrier_labels.get(k, k),
)

route_limit = st.sidebar.slider("Top-N delay routes", 5, 100, 25, step=5)
sample_size = st.sidebar.slider("Recent flights sample", 25, 500, 200, step=25)

# -----------------------------------------------------------------------------
# Header KPIs
# -----------------------------------------------------------------------------
st.title("FlightPulse — Carrier On-Time Performance")
st.caption(
    f"Window: **{start_date.isoformat()} → {end_date.isoformat()}**  ·  "
    f"{len(selected_carriers) or 'all'} carriers"
)

kpis = _kpis(start_date, end_date)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Scheduled", f"{int(kpis['flights_scheduled']):,}")
c2.metric("Completed", f"{int(kpis['flights_completed']):,}")
c3.metric("Cancelled", f"{int(kpis['flights_cancelled']):,}")
c4.metric("Avg OTP %", f"{kpis['on_time_pct']:.1f}")

# -----------------------------------------------------------------------------
# Daily OTP chart
# -----------------------------------------------------------------------------
st.subheader("Daily on-time %")
otp = _otp(start_date, end_date, tuple(selected_carriers))
if otp.empty:
    st.info("No agg_carrier_otp_daily rows in the selected window.")
else:
    chart_df = (
        otp.pivot_table(
            index="flight_date",
            columns="carrier_key",
            values="on_time_pct",
            aggfunc="mean",
        )
        .sort_index()
    )
    st.line_chart(chart_df, height=320, use_container_width=True)
    with st.expander("Raw daily aggregates"):
        st.dataframe(otp, use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# Top-N delay routes
# -----------------------------------------------------------------------------
st.subheader(f"Top {route_limit} routes by mean arrival delay")
routes = _routes(start_date, end_date, route_limit)
if routes.empty:
    st.info("No fct_flight_event rows match the route filter.")
else:
    st.dataframe(routes, use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# Recent-flights drill-down
# -----------------------------------------------------------------------------
st.subheader("Recent flights")
flight_date = st.date_input("Flight date", value=end_date, max_value=today, key="recent_date")
carrier = st.selectbox(
    "Carrier",
    options=list(carrier_labels.keys()) or [""],
    format_func=lambda k: carrier_labels.get(k, k),
    key="recent_carrier",
)
if carrier:
    recent = _recent(flight_date, carrier, sample_size)
    if recent.empty:
        st.info(f"No flights for {carrier} on {flight_date.isoformat()}.")
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True)
