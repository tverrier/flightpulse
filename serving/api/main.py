"""FastAPI app exposing /predict over silver Iceberg + LightGBM.

Run locally: `make api`  →  http://localhost:8000/docs

Endpoints:
  GET  /healthz                            — liveness + model metadata
  POST /predict                            — full-feature predict
  POST /predict/by-id                      — identity-only; hydrates from silver

Module-level FastAPI dependencies wrap the model loader and the silver
lookup so unit tests can override them via `app.dependency_overrides[...]`
without monkeypatching globals.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status

from serving.api.model import LoadedModel, get_model
from serving.api.schemas import (
    HealthResponse,
    PredictByIdRequest,
    PredictRequest,
    PredictResponse,
)
from serving.api.silver import SilverLookup, get_lookup

LOGGER = logging.getLogger("flightpulse.serving.api")

app = FastAPI(
    title="FlightPulse /predict",
    version="0.1.0",
    description=(
        "Predicts arrival delay (minutes) for a scheduled flight using a "
        "LightGBM regressor trained on silver flight_event + aircraft_state."
    ),
)


# -----------------------------------------------------------------------------
# Dependencies (overridable in tests)
# -----------------------------------------------------------------------------
def model_dependency() -> LoadedModel:
    return get_model()


def silver_dependency() -> SilverLookup:
    return get_lookup()


ModelDep = Annotated[LoadedModel, Depends(model_dependency)]
SilverDep = Annotated[SilverLookup, Depends(silver_dependency)]


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    try:
        model = get_model()
    except FileNotFoundError as exc:
        LOGGER.warning("healthz: model not loaded: %s", exc)
        return HealthResponse(status="degraded", model_loaded=False)
    meta = model.metadata
    return HealthResponse(
        status="ok",
        model_loaded=True,
        model_trained_at=meta.get("trained_at"),
        val_mae_minutes=meta.get("val_mae_minutes"),
    )


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest, model: ModelDep) -> PredictResponse:
    features = req.model_dump()
    pred = model.predict(features)[0]
    return PredictResponse(
        predicted_arr_delay_min=round(pred, 2),
        model_version=str(model.metadata.get("trained_at", "unknown")),
        feature_source="request",
    )


@app.post("/predict/by-id", response_model=PredictResponse)
def predict_by_id(
    req: PredictByIdRequest,
    model: ModelDep,
    silver: SilverDep,
) -> PredictResponse:
    hydrated = silver.lookup_features(
        carrier_code=req.carrier_code,
        flight_number=req.flight_number,
        scheduled_dep_ts=req.scheduled_dep_ts,
    )
    if hydrated is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"no silver row for carrier={req.carrier_code} "
                f"flight={req.flight_number} on {req.scheduled_dep_ts.date()}"
            ),
        )
    features = {"carrier_code": req.carrier_code, **hydrated}
    pred = model.predict(features)[0]
    return PredictResponse(
        predicted_arr_delay_min=round(pred, 2),
        model_version=str(model.metadata.get("trained_at", "unknown")),
        feature_source="silver",
    )
