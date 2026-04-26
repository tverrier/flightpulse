"""Train and persist the LightGBM arrival-delay regressor used by /predict.

The training data is normally pulled from Snowflake silver
(`silver.flight_event` joined to `silver.aircraft_state`), but for offline
development and CI we fall back to a deterministic synthetic generator that
encodes plausible structure (peak-hour congestion, hub effects, weather
days). That keeps `make train-model && make api` working on a fresh laptop
without any cloud creds.

Run as a script:

    python -m serving.api.models.train --rows 50000 --seed 42

This writes:

    serving/api/models/arr_delay_lgbm.joblib
    serving/api/models/arr_delay_lgbm.metadata.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error

from serving.api.models.feature_spec import (
    CATEGORICAL_FEATURES,
    FEATURE_ORDER,
    NUMERIC_FEATURES,
)

LOGGER = logging.getLogger("flightpulse.serving.train")

ARTIFACT_DIR = Path(__file__).resolve().parent
ARTIFACT_PATH = ARTIFACT_DIR / "arr_delay_lgbm.joblib"
METADATA_PATH = ARTIFACT_DIR / "arr_delay_lgbm.metadata.json"

# Top-30 US carriers / hubs / spokes — enough categorical cardinality to
# exercise LightGBM's categorical handler without bloating the artifact.
_CARRIERS: tuple[str, ...] = (
    "AA", "DL", "UA", "WN", "AS", "B6", "NK", "F9", "G4", "HA",
)
_HUBS: tuple[str, ...] = (
    "ATL", "DFW", "DEN", "ORD", "LAX", "JFK", "SFO", "SEA", "LAS", "PHX",
    "MCO", "MIA", "BOS", "EWR", "IAH", "MSP", "DTW", "CLT", "SLC", "BWI",
    "FLL", "DCA", "SAN", "MDW", "PHL", "TPA", "AUS", "STL", "RDU", "BNA",
)


def synthesize_training_data(n_rows: int, seed: int) -> pd.DataFrame:
    """Build a deterministic synthetic dataset mirroring the silver feature contract.

    Encoded structure:
      - peak-hour effect: late afternoon and morning rush amplify delay
      - hub effect: large hubs add baseline delay
      - winter / summer storm seasons widen the noise
      - dep_delay_min is the strongest predictor (mostly carries through to arrival)
    """
    rng = np.random.default_rng(seed)
    carriers = rng.choice(_CARRIERS, size=n_rows)
    origins = rng.choice(_HUBS, size=n_rows)
    dests = rng.choice(_HUBS, size=n_rows)
    distance_miles = rng.integers(150, 2800, size=n_rows)
    dep_hour = rng.integers(0, 24, size=n_rows)
    dow = rng.integers(0, 7, size=n_rows)
    month = rng.integers(1, 13, size=n_rows)
    dep_delay_min = rng.normal(loc=4.0, scale=18.0, size=n_rows).round().astype(int)

    peak_effect = np.where(np.isin(dep_hour, (7, 8, 17, 18, 19)), 6.0, 0.0)
    hub_effect = np.where(np.isin(origins, ("ATL", "ORD", "LAX", "EWR", "JFK")), 4.0, 0.0)
    storm_season = np.where(np.isin(month, (1, 2, 7, 8, 12)), 3.5, 0.0)
    weekend = np.where(dow >= 5, -1.5, 0.0)

    arr_delay = (
        0.85 * dep_delay_min
        + peak_effect
        + hub_effect
        + storm_season
        + weekend
        + rng.normal(loc=0.0, scale=8.0, size=n_rows)
    ).round().astype(int)

    return pd.DataFrame(
        {
            "carrier_code": carriers,
            "origin_iata": origins,
            "dest_iata": dests,
            "distance_miles": distance_miles,
            "scheduled_dep_hour": dep_hour,
            "day_of_week": dow,
            "month": month,
            "dep_delay_min": dep_delay_min,
            "arr_delay_min": arr_delay,
        }
    )


def _to_categorical(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CATEGORICAL_FEATURES:
        out[col] = out[col].astype("category")
    return out


def train(
    rows: int = 50_000,
    seed: int = 42,
    val_fraction: float = 0.2,
    artifact_path: Path = ARTIFACT_PATH,
    metadata_path: Path = METADATA_PATH,
) -> dict[str, Any]:
    df = synthesize_training_data(rows, seed)
    df = _to_categorical(df)

    n_val = int(rows * val_fraction)
    rng = np.random.default_rng(seed)
    perm = rng.permutation(rows)
    val_idx, train_idx = perm[:n_val], perm[n_val:]

    X = df[list(FEATURE_ORDER)]
    y = df["arr_delay_min"]
    X_train, y_train = X.iloc[train_idx], y.iloc[train_idx]
    X_val, y_val = X.iloc[val_idx], y.iloc[val_idx]

    model = lgb.LGBMRegressor(
        objective="regression_l1",
        n_estimators=400,
        learning_rate=0.05,
        num_leaves=63,
        min_child_samples=40,
        random_state=seed,
        n_jobs=-1,
    )
    model.fit(
        X_train,
        y_train,
        categorical_feature=list(CATEGORICAL_FEATURES),
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(stopping_rounds=25, verbose=False)],
    )

    val_pred = model.predict(X_val)
    val_mae = float(mean_absolute_error(y_val, val_pred))

    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, artifact_path)

    metadata = {
        "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "n_rows": int(rows),
        "seed": int(seed),
        "feature_order": list(FEATURE_ORDER),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "numeric_features": list(NUMERIC_FEATURES),
        "target": "arr_delay_min",
        "val_mae_minutes": round(val_mae, 3),
        "lightgbm_version": lgb.__version__,
        "best_iteration": int(model.best_iteration_ or model.n_estimators),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    LOGGER.info(
        "trained arr_delay_lgbm | rows=%d val_mae=%.2fmin best_iter=%d",
        rows, val_mae, metadata["best_iteration"],
    )
    return metadata


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--rows", type=int, default=int(os.environ.get("TRAIN_ROWS", 50_000)))
    p.add_argument("--seed", type=int, default=int(os.environ.get("TRAIN_SEED", 42)))
    p.add_argument("--val-fraction", type=float, default=0.2)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _parse_args(argv)
    meta = train(rows=args.rows, seed=args.seed, val_fraction=args.val_fraction)
    print(json.dumps(meta, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
