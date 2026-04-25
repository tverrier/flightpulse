"""Standalone runner that materializes the BTS Airbyte source spec.

Equivalent of `airbyte sync --source bts_source.yaml`. Drives `make backfill`
and the Dagster `bts_monthly_raw` asset. Idempotent: writes raw.csv and a
sidecar manifest; skips downstream work if sha256 matches the existing
manifest.

Usage:
    python -m ingestion.airbyte.run_bts_sync \
        --month 2024-01 \
        --bucket flightpulse-raw \
        --source ingestion/airbyte/bts_source.yaml
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
import zipfile
from string import Template
from typing import Any

import boto3
import yaml

from ingestion.streaming.http_client import (
    FatalHTTPError,
    TransientHTTPError,
    make_request,
)

LOGGER = logging.getLogger("flightpulse.bts_runner")


def _expand(value: str, env: dict[str, str]) -> str:
    return Template(value).safe_substitute(env)


def _load_spec(path: str) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def _format_url(spec: dict[str, Any], year: int, month: int) -> str:
    base = _expand(spec["url"]["base"], os.environ)
    suffix = spec["url"]["filename_pattern"].format(year=year, month=f"{month:02d}")
    return base + suffix


def _existing_manifest(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("manifest read failed: %s", e)
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--bucket", default=os.environ.get("S3_RAW_BUCKET"))
    parser.add_argument("--source", default="ingestion/airbyte/bts_source.yaml")
    parser.add_argument("--force", action="store_true",
                        help="ignore sha short-circuit; overwrite raw.csv")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not args.bucket:
        LOGGER.error("--bucket or S3_RAW_BUCKET required")
        return 2

    try:
        year_s, month_s = args.month.split("-")
        year, month = int(year_s), int(month_s)
        partition = dt.date(year, month, 1)
    except Exception:
        LOGGER.error("--month must be YYYY-MM")
        return 2

    spec = _load_spec(args.source)
    url = _format_url(spec, year, month)
    prefix = spec["destination"]["prefix"].strip("/")
    raw_key = f"{prefix}/ds={partition.isoformat()}/raw.csv"
    manifest_key = f"{prefix}/ds={partition.isoformat()}/_manifest.json"

    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL"))

    LOGGER.info("downloading %s", url)
    try:
        resp = make_request(
            "GET", url,
            connect_timeout=spec["http"]["connect_timeout_seconds"],
            read_timeout=spec["http"]["read_timeout_seconds"],
            max_attempts=spec["retry"]["max_attempts"],
        )
    except FatalHTTPError as e:
        LOGGER.error("BTS download fatal: %s", e)
        return 3
    except TransientHTTPError as e:
        LOGGER.error("BTS download transient (gave up): %s", e)
        return 4

    payload = resp.content
    sha = hashlib.sha256(payload).hexdigest()

    existing = _existing_manifest(s3, args.bucket, manifest_key)
    if existing and existing.get("sha256") == sha and not args.force:
        LOGGER.info("sha256 unchanged (%s); short-circuit", sha[:12])
        return 0

    LOGGER.info("unzipping (%d bytes)", len(payload))
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members:
            LOGGER.error("no CSV in archive: %s", zf.namelist())
            return 5
        member = members[0]
        with zf.open(member) as src:
            csv_bytes = src.read()

    LOGGER.info("uploading raw.csv → s3://%s/%s (%d bytes)",
                args.bucket, raw_key, len(csv_bytes))
    s3.put_object(Bucket=args.bucket, Key=raw_key, Body=csv_bytes,
                  ContentType="text/csv")

    manifest = {
        "source_url": url,
        "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "content_length": len(payload),
        "sha256": sha,
        "member_filename": member,
        "row_count_estimate": csv_bytes.count(b"\n"),
        "partition": partition.isoformat(),
    }
    s3.put_object(
        Bucket=args.bucket, Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    LOGGER.info("manifest written: %s", manifest_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
