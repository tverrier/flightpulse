"""LightGBM model loader with metadata-validated feature contract.

Behavior:
  * `get_model()` is process-wide cached (`functools.lru_cache`) so successive
    /predict calls reuse one Booster instance.
  * The sidecar metadata JSON is read alongside the joblib and the feature
    order is asserted against `feature_spec.FEATURE_ORDER`. A mismatch fails
    fast at startup rather than producing silently-wrong predictions —
    Incident #1 cost us months of NULL coercions, the same shape of bug at
    serving time would be worse.
  * `predict()` accepts either a single dict or a list of dicts and always
    returns a `list[float]` of predicted arrival delays in minutes.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import joblib
import pandas as pd

from serving.api.models.feature_spec import (
    CATEGORICAL_FEATURES,
    FEATURE_ORDER,
)

LOGGER = logging.getLogger("flightpulse.serving.model")

DEFAULT_ARTIFACT = Path(__file__).resolve().parent / "models" / "arr_delay_lgbm.joblib"
DEFAULT_METADATA = DEFAULT_ARTIFACT.with_suffix(".metadata.json")


@dataclass(frozen=True)
class LoadedModel:
    booster: Any
    metadata: dict[str, Any]
    artifact_path: Path

    def predict(self, rows: Sequence[dict[str, Any]] | dict[str, Any]) -> list[float]:
        if isinstance(rows, dict):
            rows = [rows]
        if not rows:
            return []
        df = pd.DataFrame(list(rows))
        missing = [c for c in FEATURE_ORDER if c not in df.columns]
        if missing:
            raise ValueError(f"missing features: {missing}")
        df = df[list(FEATURE_ORDER)]
        for col in CATEGORICAL_FEATURES:
            df[col] = df[col].astype("category")
        preds = self.booster.predict(df)
        return [float(x) for x in preds]


def _resolve_paths() -> tuple[Path, Path]:
    artifact = Path(os.environ.get("FLIGHTPULSE_MODEL_PATH", str(DEFAULT_ARTIFACT)))
    metadata = artifact.with_suffix(".metadata.json")
    return artifact, metadata


def load_model(
    artifact_path: Path | None = None,
    metadata_path: Path | None = None,
) -> LoadedModel:
    """Load an artifact + metadata pair without touching the global cache.

    Tests use this to load fixtures; production code should call `get_model()`.
    """
    if artifact_path is None or metadata_path is None:
        a, m = _resolve_paths()
        artifact_path = artifact_path or a
        metadata_path = metadata_path or m
    if not artifact_path.exists():
        raise FileNotFoundError(
            f"LightGBM artifact not found at {artifact_path} — "
            "run `make train-model` (or `python -m serving.api.models.train`) first."
        )
    if not metadata_path.exists():
        raise FileNotFoundError(
            f"Sidecar metadata not found at {metadata_path}; the artifact "
            "may have been written by an older trainer. Retrain with "
            "`make train-model`."
        )

    booster = joblib.load(artifact_path)
    metadata = json.loads(metadata_path.read_text())
    expected = list(FEATURE_ORDER)
    actual = list(metadata.get("feature_order") or [])
    if actual != expected:
        raise ValueError(
            "feature_order mismatch between artifact metadata and "
            f"feature_spec.FEATURE_ORDER. metadata={actual} expected={expected}"
        )
    LOGGER.info(
        "loaded model artifact=%s val_mae=%.2fmin trained_at=%s",
        artifact_path.name,
        float(metadata.get("val_mae_minutes", 0.0)),
        metadata.get("trained_at", "?"),
    )
    return LoadedModel(booster=booster, metadata=metadata, artifact_path=artifact_path)


@lru_cache(maxsize=1)
def get_model() -> LoadedModel:
    return load_model()


def reset_cache() -> None:
    """Clear the lru_cache — used by tests that swap artifacts at runtime."""
    get_model.cache_clear()
