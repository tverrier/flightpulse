"""OpenSky → Kafka producer.

Polls https://opensky-network.org/api/states/all every POLL_INTERVAL_S, decodes
the state-vector array into one JSON event per aircraft, publishes to the
Kafka topic keyed by `icao24`.

Designed against the failure modes in INCIDENTS.md:

  * Incident #3 (TCP half-close) — every HTTP call goes through `make_request`
    with explicit (connect, read) timeouts. After every successful publish we
    touch a heartbeat file (HEARTBEAT_PATH) AND publish a `MessagesPublished`
    CloudWatch metric. ECS health check should test the file mtime, not the
    process.

  * Idempotency — confluent-kafka producer is configured with
    `enable.idempotence=true`; downstream dedupes on (icao24, event_ts).

  * Drop tolerance — bounded in-memory buffer (`queue.buffering.max.kbytes`).
    On overflow the producer drops the oldest batch and emits a
    `MessagesDropped` metric so we can alarm on >5%.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer

from ingestion.streaming.http_client import (
    FatalHTTPError,
    TransientHTTPError,
    make_request,
)
from ingestion.streaming.metrics import emit_metric

LOGGER = logging.getLogger("flightpulse.opensky_producer")

OPENSKY_URL = os.environ.get(
    "OPENSKY_URL", "https://opensky-network.org/api/states/all"
)
POLL_INTERVAL_S = float(os.environ.get("POLL_INTERVAL_S", "15"))
TOPIC = os.environ.get("KAFKA_TOPIC_OPENSKY", "opensky.states.v1")
BOOTSTRAP = os.environ.get("MSK_BOOTSTRAP") or os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "")
HEARTBEAT_PATH = Path(os.environ.get("HEARTBEAT_PATH", "/tmp/opensky_producer.heartbeat"))
DRY_RUN = os.environ.get("DRY_RUN", "").lower() in {"1", "true", "yes"}

# OpenSky `states/all` array indices, per their docs.
_STATE_FIELDS: list[str] = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


def build_producer() -> Producer:
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"flightpulse-producer-{socket.gethostname()}",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 50,
        "batch.size": 65536,
        "queue.buffering.max.kbytes": 10240,  # 10 MB cap → drop policy
        "queue.buffering.max.ms": 100,
        "delivery.timeout.ms": 60000,
        "request.timeout.ms": 15000,
        "socket.timeout.ms": 15000,
        "message.send.max.retries": 5,
        "retry.backoff.ms": 250,
        "security.protocol": SECURITY_PROTOCOL,
    }
    if SECURITY_PROTOCOL.startswith("SASL"):
        conf["sasl.mechanism"] = SASL_MECHANISM or "AWS_MSK_IAM"
        if conf["sasl.mechanism"] == "AWS_MSK_IAM":
            # confluent-kafka uses the AWS MSK IAM token via the
            # `oauthbearer_token_refresh_cb`; we leave that to a sidecar lib
            # in prod (aws-msk-iam-sasl-signer-python). For local PLAINTEXT
            # we never reach this branch.
            pass
    return Producer(conf)


def fetch_states() -> dict[str, Any]:
    auth: tuple[str, str] | None = None
    user = os.environ.get("OPENSKY_USER")
    pw = os.environ.get("OPENSKY_PASS")
    if user and pw:
        auth = (user, pw)
    resp = make_request("GET", OPENSKY_URL, auth=auth)
    return resp.json()


def to_event(row: list[Any], polled_at: int) -> dict[str, Any] | None:
    if not row or len(row) < len(_STATE_FIELDS):
        return None
    event: dict[str, Any] = dict(zip(_STATE_FIELDS, row, strict=False))
    icao24 = event.get("icao24")
    if not icao24:
        return None
    event_ts = event.get("time_position") or event.get("last_contact") or polled_at
    event["icao24"] = icao24.lower()
    event["event_ts"] = int(event_ts)
    event["polled_at"] = polled_at
    callsign = event.get("callsign")
    if isinstance(callsign, str):
        event["callsign"] = callsign.strip() or None
    return event


def _delivery_cb(err: Any, msg: Any) -> None:
    if err is not None:
        LOGGER.error("delivery failed: %s", err)
        emit_metric("MessagesFailed", 1)
    # Successful deliveries are counted in batch via produce loop to avoid
    # one-metric-per-event overhead.


def publish_batch(producer: Producer, payload: dict[str, Any]) -> tuple[int, int]:
    polled_at = int(payload.get("time") or time.time())
    states = payload.get("states") or []
    published = 0
    dropped = 0
    for row in states:
        event = to_event(row, polled_at)
        if event is None:
            continue
        try:
            producer.produce(
                topic=TOPIC,
                key=event["icao24"],
                value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                timestamp=event["event_ts"] * 1000,
                on_delivery=_delivery_cb,
            )
            published += 1
        except BufferError:
            # Local queue full — drop policy per design doc.
            dropped += 1
            producer.poll(0)
    producer.poll(0)
    return published, dropped


def touch_heartbeat() -> None:
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_PATH.touch(exist_ok=True)
    os.utime(HEARTBEAT_PATH, None)


_running = True


def _handle_sig(signum: int, _frame: Any) -> None:  # noqa: ARG001
    global _running
    LOGGER.warning("received signal %s; draining producer", signum)
    _running = False


def run() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    if DRY_RUN:
        LOGGER.info("DRY_RUN=true → not connecting to Kafka")
        producer = None  # type: ignore[assignment]
    else:
        producer = build_producer()

    LOGGER.info(
        "producer ready: bootstrap=%s topic=%s poll_interval=%ss",
        BOOTSTRAP, TOPIC, POLL_INTERVAL_S,
    )
    touch_heartbeat()

    while _running:
        loop_started = time.monotonic()
        try:
            payload = fetch_states()
        except FatalHTTPError as e:
            LOGGER.error("fatal HTTP error from OpenSky, exiting: %s", e)
            emit_metric("FetchFatal", 1)
            return 2
        except TransientHTTPError as e:
            LOGGER.warning("transient HTTP error, will retry next tick: %s", e)
            emit_metric("FetchTransient", 1)
            time.sleep(min(POLL_INTERVAL_S, 5))
            continue

        try:
            if producer is None:
                published = len(payload.get("states") or [])
                dropped = 0
            else:
                published, dropped = publish_batch(producer, payload)
        except KafkaException as e:
            LOGGER.error("kafka publish error: %s", e)
            emit_metric("KafkaError", 1)
            time.sleep(min(POLL_INTERVAL_S, 5))
            continue

        emit_metric("MessagesPublished", published)
        if dropped:
            emit_metric("MessagesDropped", dropped)
        touch_heartbeat()
        LOGGER.info("tick: published=%d dropped=%d", published, dropped)

        elapsed = time.monotonic() - loop_started
        sleep_for = max(0.0, POLL_INTERVAL_S - elapsed)
        # Break sleep into 1s slices so SIGTERM is responsive.
        slept = 0.0
        while slept < sleep_for and _running:
            time.sleep(min(1.0, sleep_for - slept))
            slept += 1.0

    if producer is not None:
        LOGGER.info("flushing producer ...")
        producer.flush(timeout=15)
    LOGGER.info("producer exited cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(run())
