"""Kafka → S3 Parquet sink for OpenSky state vectors.

Rolls one Parquet file per minute under
  s3://${S3_RAW_BUCKET}/opensky/ds=YYYY-MM-DD/hr=HH/<minute>-<uuid>.parquet

Per ARCHITECTURE.md and PIPELINE.md:
  * keyed-by-icao24, partition assignment guarantees per-aircraft order
  * append-only writes; downstream dedupes on (icao24, event_ts)
  * unparseable payloads → DLQ topic `opensky.states.dlq`
  * heartbeat published per flush to FlightPulse/OpenSkyConsumer
"""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, Producer

from ingestion.streaming.metrics import emit_metric

LOGGER = logging.getLogger("flightpulse.opensky_consumer")

TOPIC = os.environ.get("KAFKA_TOPIC_OPENSKY", "opensky.states.v1")
DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_OPENSKY_DLQ", "opensky.states.dlq")
BOOTSTRAP = os.environ.get("MSK_BOOTSTRAP") or os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "flightpulse-consumer")
SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
S3_BUCKET = os.environ.get("S3_RAW_BUCKET", "flightpulse-raw")
S3_PREFIX = os.environ.get("S3_OPENSKY_PREFIX", "opensky")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL")  # set for MinIO local dev
ROLL_INTERVAL_S = int(os.environ.get("ROLL_INTERVAL_S", "60"))
MAX_BATCH = int(os.environ.get("MAX_BATCH", "5000"))

# Schema for the rolled Parquet file. Keep it permissive; downstream casts.
_SCHEMA = pa.schema([
    pa.field("icao24", pa.string(), nullable=False),
    pa.field("callsign", pa.string()),
    pa.field("origin_country", pa.string()),
    pa.field("event_ts", pa.int64(), nullable=False),
    pa.field("polled_at", pa.int64()),
    pa.field("time_position", pa.int64()),
    pa.field("last_contact", pa.int64()),
    pa.field("longitude", pa.float64()),
    pa.field("latitude", pa.float64()),
    pa.field("baro_altitude", pa.float64()),
    pa.field("on_ground", pa.bool_()),
    pa.field("velocity", pa.float64()),
    pa.field("true_track", pa.float64()),
    pa.field("vertical_rate", pa.float64()),
    pa.field("geo_altitude", pa.float64()),
    pa.field("squawk", pa.string()),
    pa.field("spi", pa.bool_()),
    pa.field("position_source", pa.int64()),
])


def build_consumer() -> Consumer:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"flightpulse-consumer-{socket.gethostname()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit after S3 flush succeeds
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
        "fetch.min.bytes": 1024,
        "security.protocol": SECURITY_PROTOCOL,
    }
    return Consumer(conf)


def build_dlq() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"flightpulse-dlq-{socket.gethostname()}",
        "compression.type": "zstd",
        "security.protocol": SECURITY_PROTOCOL,
    })


def s3_client() -> Any:
    kwargs: dict[str, Any] = {}
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)


def _coerce_row(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {f.name: None for f in _SCHEMA}
    for k, v in raw.items():
        if k in out:
            out[k] = v
    return out


def write_parquet(rows: list[dict[str, Any]], s3: Any) -> str:
    if not rows:
        return ""
    table = pa.Table.from_pylist([_coerce_row(r) for r in rows], schema=_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)

    now = datetime.now(timezone.utc)
    minute = now.strftime("%H%M")
    key = (
        f"{S3_PREFIX}/ds={now.strftime('%Y-%m-%d')}"
        f"/hr={now.strftime('%H')}/{minute}-{uuid.uuid4().hex[:8]}.parquet"
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    LOGGER.info("flushed %d rows to s3://%s/%s (%d bytes)",
                len(rows), S3_BUCKET, key, buf.tell())
    return key


_running = True


def _handle_sig(signum: int, _frame: Any) -> None:  # noqa: ARG001
    global _running
    LOGGER.warning("received signal %s; will flush and exit", signum)
    _running = False


def run() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    consumer = build_consumer()
    consumer.subscribe([TOPIC])
    dlq = build_dlq()
    s3 = s3_client()

    LOGGER.info("consumer ready: bootstrap=%s topic=%s group=%s sink=s3://%s/%s",
                BOOTSTRAP, TOPIC, GROUP_ID, S3_BUCKET, S3_PREFIX)

    buffer: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    LOGGER.error("consumer error: %s", msg.error())
                    emit_metric("ConsumerError", 1)
            else:
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    buffer.append(payload)
                except Exception as e:  # noqa: BLE001
                    LOGGER.warning("DLQ unparseable msg from %s: %s",
                                   msg.topic(), e)
                    dlq.produce(DLQ_TOPIC, value=msg.value(),
                                key=msg.key())
                    dlq.poll(0)
                    emit_metric("MessagesDLQ", 1)

            now = time.monotonic()
            should_roll = (
                buffer and (
                    len(buffer) >= MAX_BATCH
                    or (now - last_flush) >= ROLL_INTERVAL_S
                )
            )
            if should_roll:
                try:
                    write_parquet(buffer, s3)
                    consumer.commit(asynchronous=False)
                    emit_metric("RowsFlushed", len(buffer))
                    buffer.clear()
                    last_flush = now
                except Exception as e:  # noqa: BLE001
                    LOGGER.exception("flush failed: %s", e)
                    emit_metric("FlushError", 1)
                    # Do NOT commit; messages will be redelivered.
                    time.sleep(2)
    finally:
        if buffer:
            try:
                write_parquet(buffer, s3)
                consumer.commit(asynchronous=False)
            except Exception:  # noqa: BLE001
                LOGGER.exception("final flush failed")
        consumer.close()
        dlq.flush(timeout=10)
    LOGGER.info("consumer exited cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(run())
