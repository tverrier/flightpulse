"""Partition definitions used across assets and jobs.

BTS lands one CSV per calendar month; OpenFlights is treated as a quarterly
seed (we tag the manifest with the run date but don't partition the asset).
"""

from __future__ import annotations

from dagster import MonthlyPartitionsDefinition

# BTS publishes the prior month around the 5th. Start the partition catalog
# at 2009-01 — that's the earliest month available on transtats.bts.gov for
# the on-time performance file. End is open (Dagster computes the latest).
BTS_MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(
    start_date="2009-01-01",
    timezone="UTC",
    fmt="%Y-%m-01",
    end_offset=0,
)
