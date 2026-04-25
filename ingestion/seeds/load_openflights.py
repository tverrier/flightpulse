"""Quarterly seed loader: OpenFlights airports + airlines → S3.

Writes:
  s3://${S3_RAW_BUCKET}/openflights/ds=YYYY-MM-DD/airports.csv
  s3://${S3_RAW_BUCKET}/openflights/ds=YYYY-MM-DD/airlines.csv
  s3://${S3_RAW_BUCKET}/openflights/ds=YYYY-MM-DD/_manifest.json

The schemas (column names) come from the OpenFlights data dictionary at
https://openflights.org/data.html — the upstream files are headerless, so we
prepend headers as the first line on upload to make Glue / dbt happy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import sys
from typing import Iterable

import boto3

from ingestion.streaming.http_client import make_request

LOGGER = logging.getLogger("flightpulse.openflights_seed")

AIRPORTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
AIRLINES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"

AIRPORTS_HEADER = ",".join([
    "openflights_id", "name", "city", "country", "iata", "icao",
    "latitude", "longitude", "altitude", "timezone_offset", "dst",
    "tz_database", "type", "source",
])
AIRLINES_HEADER = ",".join([
    "openflights_id", "name", "alias", "iata", "icao",
    "callsign", "country", "active",
])


def _prepend_header(header: str, body: bytes) -> bytes:
    return (header + "\n").encode("utf-8") + body


def _put(s3, bucket: str, key: str, body: bytes, ctype: str) -> str:
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ctype)
    LOGGER.info("uploaded s3://%s/%s (%d bytes)", bucket, key, len(body))
    return key


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.environ.get("S3_RAW_BUCKET"))
    parser.add_argument("--prefix", default="openflights")
    parser.add_argument("--ds", default=dt.date.today().isoformat())
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if not args.bucket:
        LOGGER.error("--bucket or S3_RAW_BUCKET required")
        return 2

    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL"))

    airports = make_request("GET", AIRPORTS_URL).content
    airlines = make_request("GET", AIRLINES_URL).content

    airports_body = _prepend_header(AIRPORTS_HEADER, airports)
    airlines_body = _prepend_header(AIRLINES_HEADER, airlines)

    base = f"{args.prefix}/ds={args.ds}"
    _put(s3, args.bucket, f"{base}/airports.csv", airports_body, "text/csv")
    _put(s3, args.bucket, f"{base}/airlines.csv", airlines_body, "text/csv")

    manifest = {
        "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "airports": {
            "url": AIRPORTS_URL,
            "sha256": hashlib.sha256(airports).hexdigest(),
            "byte_count": len(airports),
        },
        "airlines": {
            "url": AIRLINES_URL,
            "sha256": hashlib.sha256(airlines).hexdigest(),
            "byte_count": len(airlines),
        },
    }
    _put(
        s3, args.bucket, f"{base}/_manifest.json",
        json.dumps(manifest, indent=2).encode("utf-8"),
        "application/json",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
