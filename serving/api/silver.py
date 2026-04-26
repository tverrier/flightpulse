"""pyiceberg-backed feature lookup against `silver.flight_event`.

ARCHITECTURE.md spec: "FastAPI /predict reads silver Iceberg via pyiceberg
and a cached LightGBM model." When a caller already has full features in
hand, /predict serves them directly. When they only have an identity tuple
(carrier_code, flight_number, scheduled_dep_ts), we look up the missing
distance / hour / day-of-week / month from silver and combine.

Catalog config follows SETUP.md §8: the Glue catalog is named by
`GLUE_DATABASE` and `S3_SILVER_BUCKET` is where silver lives. Imports are
deferred so importing this module on a machine without pyiceberg installed
(e.g. a stripped-down CI image that only runs unit tests against a stub
loader) doesn't blow up at import time.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol

LOGGER = logging.getLogger("flightpulse.serving.silver")


class SilverLookup(Protocol):
    def lookup_features(
        self,
        carrier_code: str,
        flight_number: str,
        scheduled_dep_ts: dt.datetime,
    ) -> dict[str, Any] | None: ...


@dataclass
class IcebergSilverLookup:
    """Real pyiceberg-backed implementation.

    Lazy-builds a Catalog handle on first use, then caches the loaded table.
    Hidden-partitioned on `month(flight_date)` so scans for a single
    (carrier, flight_number, date) hit one partition cheaply.
    """

    glue_database: str
    silver_bucket: str
    table_name: str = "flight_event"
    region: str = "us-east-1"

    _catalog: Any = None
    _table: Any = None

    def _load_table(self) -> Any:
        if self._table is not None:
            return self._table
        from pyiceberg.catalog import load_catalog  # type: ignore[import-not-found]

        if self._catalog is None:
            self._catalog = load_catalog(
                "glue",
                **{
                    "type": "glue",
                    "warehouse": f"s3://{self.silver_bucket}/",
                    "region_name": self.region,
                },
            )
        self._table = self._catalog.load_table(f"{self.glue_database}.{self.table_name}")
        return self._table

    def lookup_features(
        self,
        carrier_code: str,
        flight_number: str,
        scheduled_dep_ts: dt.datetime,
    ) -> dict[str, Any] | None:
        from pyiceberg.expressions import And, EqualTo  # type: ignore[import-not-found]

        flight_date = scheduled_dep_ts.date()
        table = self._load_table()
        scan = table.scan(
            row_filter=And(
                EqualTo("carrier_code", carrier_code),
                EqualTo("flight_number", str(flight_number)),
                EqualTo("flight_date", flight_date),
            ),
            selected_fields=(
                "distance_miles",
                "scheduled_dep_ts",
                "origin_iata",
                "dest_iata",
                "dep_delay_min",
            ),
            limit=1,
        )
        rows = list(scan.to_arrow().to_pylist())
        if not rows:
            LOGGER.info(
                "silver lookup miss carrier=%s flight=%s date=%s",
                carrier_code, flight_number, flight_date,
            )
            return None
        row = rows[0]
        sched: dt.datetime = row["scheduled_dep_ts"]
        return {
            "distance_miles": int(row["distance_miles"]),
            "scheduled_dep_hour": int(sched.hour),
            "day_of_week": int(sched.weekday()),
            "month": int(sched.month),
            "origin_iata": str(row["origin_iata"]),
            "dest_iata": str(row["dest_iata"]),
            "dep_delay_min": int(row["dep_delay_min"]) if row.get("dep_delay_min") is not None else 0,
        }


@lru_cache(maxsize=1)
def get_lookup() -> SilverLookup:
    return IcebergSilverLookup(
        glue_database=os.environ.get("GLUE_DATABASE", "flightpulse_silver"),
        silver_bucket=os.environ.get("S3_SILVER_BUCKET", "flightpulse-silver"),
        region=os.environ.get("AWS_REGION", "us-east-1"),
    )


def reset_cache() -> None:
    get_lookup.cache_clear()
