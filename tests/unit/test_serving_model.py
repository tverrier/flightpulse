"""Tests for the LightGBM training script + model loader."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("lightgbm")
pytest.importorskip("joblib")
pytest.importorskip("sklearn")

from serving.api import model as model_module  # noqa: E402
from serving.api.models import train as train_module  # noqa: E402
from serving.api.models.feature_spec import FEATURE_ORDER  # noqa: E402


# -----------------------------------------------------------------------------
# train.synthesize_training_data
# -----------------------------------------------------------------------------
def test_synthesize_training_data_is_deterministic():
    a = train_module.synthesize_training_data(500, seed=7)
    b = train_module.synthesize_training_data(500, seed=7)
    assert a.equals(b)


def test_synthesize_training_data_columns_match_feature_spec():
    df = train_module.synthesize_training_data(50, seed=1)
    for col in FEATURE_ORDER:
        assert col in df.columns
    assert "arr_delay_min" in df.columns


# -----------------------------------------------------------------------------
# train.train round-trip + loader contract
# -----------------------------------------------------------------------------
def test_train_writes_artifact_and_metadata(tmp_path: Path):
    artifact = tmp_path / "arr_delay_lgbm.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    meta = train_module.train(
        rows=2_500,
        seed=0,
        val_fraction=0.2,
        artifact_path=artifact,
        metadata_path=metadata,
    )
    assert artifact.exists()
    assert metadata.exists()
    on_disk = json.loads(metadata.read_text())
    assert on_disk == meta
    assert on_disk["feature_order"] == list(FEATURE_ORDER)
    assert on_disk["target"] == "arr_delay_min"
    assert 0.0 < on_disk["val_mae_minutes"] < 30.0


def test_load_model_validates_feature_order_mismatch(tmp_path: Path):
    artifact = tmp_path / "arr_delay_lgbm.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    train_module.train(rows=1_500, seed=0, artifact_path=artifact, metadata_path=metadata)

    bad = json.loads(metadata.read_text())
    bad["feature_order"] = ["something_else", *bad["feature_order"][1:]]
    metadata.write_text(json.dumps(bad))

    with pytest.raises(ValueError, match="feature_order mismatch"):
        model_module.load_model(artifact_path=artifact, metadata_path=metadata)


def test_load_model_predict_returns_floats(tmp_path: Path):
    artifact = tmp_path / "arr_delay_lgbm.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    train_module.train(rows=2_000, seed=0, artifact_path=artifact, metadata_path=metadata)

    loaded = model_module.load_model(artifact_path=artifact, metadata_path=metadata)
    out = loaded.predict(
        {
            "carrier_code": "WN",
            "origin_iata": "DEN",
            "dest_iata": "LAX",
            "distance_miles": 863,
            "scheduled_dep_hour": 8,
            "day_of_week": 1,
            "month": 6,
            "dep_delay_min": 12,
        }
    )
    assert isinstance(out, list) and len(out) == 1
    assert isinstance(out[0], float)


def test_load_model_predict_rejects_missing_features(tmp_path: Path):
    artifact = tmp_path / "arr_delay_lgbm.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    train_module.train(rows=1_500, seed=0, artifact_path=artifact, metadata_path=metadata)
    loaded = model_module.load_model(artifact_path=artifact, metadata_path=metadata)
    with pytest.raises(ValueError, match="missing features"):
        loaded.predict({"carrier_code": "WN"})


def test_load_model_raises_when_artifact_missing(tmp_path: Path):
    artifact = tmp_path / "missing.joblib"
    metadata = artifact.with_suffix(".metadata.json")
    with pytest.raises(FileNotFoundError, match="artifact not found"):
        model_module.load_model(artifact_path=artifact, metadata_path=metadata)
