"""LightGBM model artifact directory.

The trained joblib lives at `arr_delay_lgbm.joblib` alongside a sibling
`arr_delay_lgbm.metadata.json` describing the feature contract. Both are
gitignored — produce them with `make train-model` before `make api`.
"""

from serving.api.models.feature_spec import (
    CATEGORICAL_FEATURES,
    FEATURE_ORDER,
    NUMERIC_FEATURES,
)

__all__ = ["FEATURE_ORDER", "CATEGORICAL_FEATURES", "NUMERIC_FEATURES"]
