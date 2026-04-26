"""Pydantic request/response schemas for /predict.

Two request shapes are accepted:

  * **features-in-request** (`PredictRequest`): caller supplies the full
    feature row. Used by upstream services that already have them, and by
    tests.
  * **identity-only** (`PredictByIdRequest`): caller supplies the natural
    key of a flight; the API hydrates features from silver.flight_event via
    pyiceberg.

Keeping them as separate models lets FastAPI emit a tight OpenAPI schema
and lets us validate each shape independently.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

_CARRIER_RE = re.compile(r"^[A-Z0-9]{2,3}$")
_IATA_RE = re.compile(r"^[A-Z]{3}$")


class PredictRequest(BaseModel):
    """Full-feature predict request — feature contract matches feature_spec."""

    carrier_code: str = Field(..., examples=["WN"])
    origin_iata: str = Field(..., examples=["DEN"])
    dest_iata: str = Field(..., examples=["LAX"])
    distance_miles: int = Field(..., ge=1, le=15_000, examples=[863])
    scheduled_dep_hour: int = Field(..., ge=0, le=23, examples=[8])
    day_of_week: int = Field(..., ge=0, le=6, description="Monday=0", examples=[1])
    month: int = Field(..., ge=1, le=12, examples=[1])
    dep_delay_min: int = Field(0, ge=-60, le=720, examples=[12])

    @field_validator("carrier_code")
    @classmethod
    def _carrier_format(cls, v: str) -> str:
        v = v.upper()
        if not _CARRIER_RE.fullmatch(v):
            raise ValueError("carrier_code must be 2-3 uppercase alphanumeric chars")
        return v

    @field_validator("origin_iata", "dest_iata")
    @classmethod
    def _iata_format(cls, v: str) -> str:
        v = v.upper()
        if not _IATA_RE.fullmatch(v):
            raise ValueError("airport code must be 3 uppercase letters (IATA)")
        return v


class PredictByIdRequest(BaseModel):
    """Identity-only predict request — features are hydrated from silver."""

    carrier_code: str = Field(..., examples=["WN"])
    flight_number: str = Field(..., examples=["1234"])
    scheduled_dep_ts: dt.datetime = Field(..., examples=["2026-04-25T08:30:00-07:00"])

    @field_validator("carrier_code")
    @classmethod
    def _carrier_format(cls, v: str) -> str:
        v = v.upper()
        if not _CARRIER_RE.fullmatch(v):
            raise ValueError("carrier_code must be 2-3 uppercase alphanumeric chars")
        return v


class PredictResponse(BaseModel):
    predicted_arr_delay_min: float
    model_version: str
    feature_source: Literal["request", "silver"]


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    model_loaded: bool
    model_trained_at: str | None = None
    val_mae_minutes: float | None = None
