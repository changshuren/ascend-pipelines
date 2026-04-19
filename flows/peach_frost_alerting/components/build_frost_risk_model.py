from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.resources import ref, test, transform


FEATURE_COLUMNS = [
    "temperature_f",
    "dew_point_f",
    "dewpoint_depression_f",
    "relative_humidity",
    "wind_speed_mph",
    "precipitation_in",
    "soil_temp_gap_f",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "temp_change_1h",
    "temp_change_3h",
    "temp_mean_last_6h",
    "temp_min_last_6h",
    "temp_mean_last_12h",
    "temp_min_last_12h",
    "humidity_mean_last_6h",
    "humidity_mean_last_12h",
    "wind_mean_last_6h",
    "wind_mean_last_12h",
    "hours_below_freezing_last_12h",
    "hours_below_frost_threshold_last_12h",
    "warming_after_cold_flag",
]


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(value, -30.0, 30.0)))


@transform(
    inputs=[ref("build_historical_frost_features")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="frost_risk_score"),
    ],
)
def build_frost_risk_model(
    build_historical_frost_features: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    data = build_historical_frost_features.copy()

    for column in FEATURE_COLUMNS + ["frost_event", "temperature_f"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    feature_frame = data[FEATURE_COLUMNS].copy()
    medians = feature_frame.median(numeric_only=True).fillna(0.0)
    feature_frame = feature_frame.fillna(medians)
    target = data["frost_event"].fillna(0.0).clip(lower=0.0, upper=1.0)

    standardized = (feature_frame - medians) / feature_frame.std(ddof=0).replace(0, 1).fillna(1)
    standardized = standardized.fillna(0.0)
    target_centered = target - target.mean()
    coefficients = standardized.mul(target_centered, axis=0).mean().fillna(0.0)

    base_temperature_signal = np.clip((28.0 - feature_frame["temperature_f"]) / 10.0, -2.0, 2.0)
    trend_signal = np.clip((28.0 - feature_frame["temp_min_last_12h"]) / 10.0, -2.0, 2.0)
    humidity_signal = np.clip((feature_frame["relative_humidity"] - 70.0) / 30.0, -1.5, 1.5)
    wind_signal = np.clip((feature_frame["wind_speed_mph"] - 5.0) / 15.0, -1.5, 1.5)
    learned_signal = standardized.mul(coefficients, axis=1).sum(axis=1)
    intercept = float(np.log((target.mean() + 1e-6) / (1.0 - target.mean() + 1e-6)))

    logits = intercept + (1.8 * base_temperature_signal) + (0.9 * trend_signal) + (0.3 * humidity_signal) + (0.2 * wind_signal) + learned_signal
    probabilities = _sigmoid(logits.to_numpy(dtype=float))
    threshold = 0.4

    data["frost_risk_score"] = probabilities
    data["frost_event_predicted"] = (data["temperature_f"].lt(28.0) | (data["frost_risk_score"] >= threshold)).astype(bool)

    mae = float(np.mean(np.abs(target.to_numpy(dtype=float) - probabilities)))
    brier_score = float(np.mean((target.to_numpy(dtype=float) - probabilities) ** 2))

    data["model_threshold"] = threshold
    data["predicted_temp_f"] = data["temperature_f"]
    data["model_feature_list"] = json.dumps(FEATURE_COLUMNS)
    data["feature_medians_json"] = json.dumps({key: float(value) for key, value in medians.to_dict().items()})
    data["feature_scales_json"] = json.dumps(
        {
            key: float(value)
            for key, value in feature_frame.std(ddof=0).replace(0, 1).fillna(1).to_dict().items()
        }
    )
    data["model_coefficients_json"] = json.dumps(
        {
            "intercept": intercept,
            "base_temperature_weight": 1.8,
            "trend_weight": 0.9,
            "humidity_weight": 0.3,
            "wind_weight": 0.2,
            **{feature: float(value) for feature, value in coefficients.to_dict().items()},
        }
    )
    data["model_mae"] = mae
    data["model_brier_score"] = brier_score
    output_columns = [
        "location_id",
        "location_name",
        "state_province",
        "timestamp",
        "temperature_f",
        "predicted_temp_f",
        "frost_event",
        "frost_risk_score",
        "frost_event_predicted",
        "model_threshold",
        "model_feature_list",
        "feature_medians_json",
        "feature_scales_json",
        "model_coefficients_json",
        "model_mae",
        "model_brier_score",
    ]
    return data[output_columns].drop_duplicates().reset_index(drop=True)