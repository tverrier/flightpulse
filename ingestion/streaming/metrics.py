"""Tiny CloudWatch publisher.

Falls back to logger.info when:
  * boto3 is unavailable
  * AWS creds are absent
  * METRICS_NAMESPACE is unset

This keeps local docker-compose runs noise-free while letting Fargate tasks
publish to FlightPulse/OpenSkyProducer + FlightPulse/OpenSkyConsumer.
"""

from __future__ import annotations

import logging
import os
from typing import Any

LOGGER = logging.getLogger("flightpulse.metrics")

NAMESPACE = os.environ.get("METRICS_NAMESPACE", "FlightPulse/OpenSkyProducer")
COMPONENT = os.environ.get("METRICS_COMPONENT", "producer")

_client: Any = None
_disabled = False


def _get_client() -> Any:
    global _client, _disabled
    if _disabled:
        return None
    if _client is not None:
        return _client
    try:
        import boto3  # noqa: WPS433 — local import keeps unit tests light

        _client = boto3.client("cloudwatch")
        return _client
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("CloudWatch metrics disabled: %s", e)
        _disabled = True
        return None


def emit_metric(name: str, value: float, unit: str = "Count") -> None:
    client = _get_client()
    if client is None:
        LOGGER.info("metric %s.%s=%s", NAMESPACE, name, value)
        return
    try:
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": float(value),
                    "Unit": unit,
                    "Dimensions": [{"Name": "Component", "Value": COMPONENT}],
                }
            ],
        )
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("failed to emit metric %s: %s", name, e)
