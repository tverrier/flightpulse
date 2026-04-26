"""FastAPI surface tests using TestClient + dependency overrides."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("lightgbm")

from fastapi.testclient import TestClient  # noqa: E402

from serving.api import main as api_main  # noqa: E402
from serving.api import model as model_module  # noqa: E402
from serving.api.models import train as train_module  # noqa: E402


@pytest.fixture(scope="module")
def loaded_model(tmp_path_factory: pytest.TempPathFactory) -> model_module.LoadedModel:
    tmp_path = tmp_path_factory.mktemp("model")
    artifact = tmp_path / "arr_delay_lgbm.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    train_module.train(rows=2_000, seed=0, artifact_path=artifact, metadata_path=metadata)
    return model_module.load_model(artifact_path=artifact, metadata_path=metadata)


class _StubSilver:
    """In-memory silver lookup that returns a fixed feature row."""

    def __init__(self, payload: dict[str, Any] | None) -> None:
        self.payload = payload
        self.calls: list[tuple[str, str, dt.datetime]] = []

    def lookup_features(self, carrier_code: str, flight_number: str, scheduled_dep_ts: dt.datetime):
        self.calls.append((carrier_code, flight_number, scheduled_dep_ts))
        return self.payload


@pytest.fixture
def client(loaded_model: model_module.LoadedModel):
    api_main.app.dependency_overrides[api_main.model_dependency] = lambda: loaded_model
    yield TestClient(api_main.app)
    api_main.app.dependency_overrides.clear()


# -----------------------------------------------------------------------------
# /healthz
# -----------------------------------------------------------------------------
def test_healthz_ok(client: TestClient, loaded_model, monkeypatch):
    # /healthz reads the cached singleton, not the override — point it at our fixture.
    monkeypatch.setattr(api_main, "get_model", lambda: loaded_model)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["model_loaded"] is True
    assert body["val_mae_minutes"] is not None


def test_healthz_degraded_when_artifact_missing(client: TestClient, monkeypatch):
    def raise_missing() -> Any:
        raise FileNotFoundError("no artifact")

    monkeypatch.setattr(api_main, "get_model", raise_missing)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["model_loaded"] is False


# -----------------------------------------------------------------------------
# /predict
# -----------------------------------------------------------------------------
_VALID_REQ = {
    "carrier_code": "WN",
    "origin_iata": "DEN",
    "dest_iata": "LAX",
    "distance_miles": 863,
    "scheduled_dep_hour": 8,
    "day_of_week": 1,
    "month": 6,
    "dep_delay_min": 12,
}


def test_predict_returns_response(client: TestClient):
    resp = client.post("/predict", json=_VALID_REQ)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["feature_source"] == "request"
    assert isinstance(body["predicted_arr_delay_min"], float)
    # Sanity: a 12-minute departure delay should not produce a wildly negative or
    # >12h arrival delay prediction. The synthetic generator caps the natural range.
    assert -120.0 < body["predicted_arr_delay_min"] < 240.0


def test_predict_validates_iata(client: TestClient):
    bad = dict(_VALID_REQ)
    bad["origin_iata"] = "Denver"
    resp = client.post("/predict", json=bad)
    assert resp.status_code == 422


def test_predict_validates_hour_range(client: TestClient):
    bad = dict(_VALID_REQ)
    bad["scheduled_dep_hour"] = 99
    resp = client.post("/predict", json=bad)
    assert resp.status_code == 422


# -----------------------------------------------------------------------------
# /predict/by-id — silver hydration via stub
# -----------------------------------------------------------------------------
def test_predict_by_id_hydrates_from_silver(client: TestClient):
    stub = _StubSilver(
        {
            "distance_miles": 863,
            "scheduled_dep_hour": 8,
            "day_of_week": 1,
            "month": 6,
            "origin_iata": "DEN",
            "dest_iata": "LAX",
            "dep_delay_min": 0,
        }
    )
    api_main.app.dependency_overrides[api_main.silver_dependency] = lambda: stub
    try:
        resp = client.post(
            "/predict/by-id",
            json={
                "carrier_code": "WN",
                "flight_number": "1234",
                "scheduled_dep_ts": "2026-04-25T08:30:00",
            },
        )
    finally:
        api_main.app.dependency_overrides.pop(api_main.silver_dependency, None)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["feature_source"] == "silver"
    assert stub.calls and stub.calls[0][0] == "WN"


def test_predict_by_id_404_when_silver_misses(client: TestClient):
    api_main.app.dependency_overrides[api_main.silver_dependency] = lambda: _StubSilver(None)
    try:
        resp = client.post(
            "/predict/by-id",
            json={
                "carrier_code": "WN",
                "flight_number": "9999",
                "scheduled_dep_ts": "2026-04-25T08:30:00",
            },
        )
    finally:
        api_main.app.dependency_overrides.pop(api_main.silver_dependency, None)
    assert resp.status_code == 404
    assert "no silver row" in resp.json()["detail"]
