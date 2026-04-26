"""Feature contract shared between training and inference.

Keeping the column order, categorical column set, and numeric column set in
one place means a feature added at training time can never silently disagree
with what the API marshals into a DataFrame at predict time. The training
script writes these names into the sidecar metadata JSON; the API reads them
back and validates that the joblib's expected feature names match.
"""

from __future__ import annotations

CATEGORICAL_FEATURES: tuple[str, ...] = (
    "carrier_code",
    "origin_iata",
    "dest_iata",
)

NUMERIC_FEATURES: tuple[str, ...] = (
    "distance_miles",
    "scheduled_dep_hour",
    "day_of_week",
    "month",
    "dep_delay_min",
)

FEATURE_ORDER: tuple[str, ...] = CATEGORICAL_FEATURES + NUMERIC_FEATURES
