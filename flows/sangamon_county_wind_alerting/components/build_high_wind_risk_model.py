from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.resources import ref, test, transform


FEATURE_COLUMNS = [
    "wind_gust_mph_lag_1h",
    "wind_gust_mph_lag_3h_avg",
    "wind_gust_mph_lag_6h_max",
    "wind_speed_mph_lag_3h_avg",
    "wind_speed_mph_lag_6h_avg",
    "precipitation_in_lag_6h_sum",
    "temperature_f",
    "temperature_f_lag_3h_avg",
    "temperature_f_change_1h",
    "wind_gust_change_1h",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "hours_above_15_mph_last_12h",
    "hours_above_20_mph_last_12h",
]


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(value, -30.0, 30.0)))


@transform(
    inputs=[ref("build_historical_wind_features")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="high_wind_risk_score"),
        test("not_null", column="predicted_wind_gust_mph"),
    ],
)
def build_high_wind_risk_model(
    build_historical_wind_features: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    data = build_historical_wind_features.copy()

    for column in FEATURE_COLUMNS + ["high_wind_event", "wind_gust_mph"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    feature_frame = data[FEATURE_COLUMNS].copy()
    medians = feature_frame.median(numeric_only=True).fillna(0.0)
    scales = feature_frame.std(ddof=0).replace(0, 1).fillna(1.0)
    feature_frame = feature_frame.fillna(medians)
    target = data["high_wind_event"].fillna(0.0).clip(lower=0.0, upper=1.0)

    standardized = (feature_frame - medians) / scales
    standardized = standardized.fillna(0.0)
    target_centered = target - target.mean()
    coefficients = standardized.mul(target_centered, axis=0).mean().fillna(0.0)

    base_gust_signal = np.clip((feature_frame["wind_gust_mph_lag_3h_avg"] - 15.0) / 10.0, -2.0, 2.0)
    max_gust_signal = np.clip((feature_frame["wind_gust_mph_lag_6h_max"] - 15.0) / 10.0, -2.0, 2.0)
    sustained_signal = np.clip((feature_frame["hours_above_15_mph_last_12h"] - 2.0) / 4.0, -2.0, 2.0)
    learned_signal = standardized.mul(coefficients, axis=1).sum(axis=1)
    intercept = float(np.log((target.mean() + 1e-6) / (1.0 - target.mean() + 1e-6)))

    logits = (
        intercept
        + (1.5 * base_gust_signal)
        + (1.0 * max_gust_signal)
        + (0.7 * sustained_signal)
        + learned_signal
    )
    probabilities = _sigmoid(logits.to_numpy(dtype=float))
    threshold = 0.5

    predicted_wind_gust_mph = (
        0.45 * feature_frame["wind_gust_mph_lag_1h"]
        + 0.35 * feature_frame["wind_gust_mph_lag_3h_avg"]
        + 0.20 * feature_frame["wind_gust_mph_lag_6h_max"]
    )

    data["predicted_wind_gust_mph"] = predicted_wind_gust_mph.round(3)
    data["high_wind_risk_score"] = probabilities
    data["high_wind_event_predicted"] = (
        data["predicted_wind_gust_mph"].gt(15.0)
        | data["high_wind_risk_score"].ge(threshold)
    ).astype(int)
    data["model_version"] = "sangamon_wind_v1"
    data["trained_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    data["training_row_count"] = len(data)
    data["positive_event_count"] = int(target.sum())
    data["threshold_used"] = threshold
    data["model_feature_list"] = json.dumps(FEATURE_COLUMNS)
    data["feature_medians_json"] = json.dumps({key: float(value) for key, value in medians.to_dict().items()})
    data["feature_scales_json"] = json.dumps({key: float(value) for key, value in scales.to_dict().items()})
    data["model_coefficients_json"] = json.dumps(
        {
            "intercept": intercept,
            "base_gust_weight": 1.5,
            "max_gust_weight": 1.0,
            "sustained_weight": 0.7,
            **{feature: float(value) for feature, value in coefficients.to_dict().items()},
        }
    )
    data["model_brier_score"] = float(np.mean((target.to_numpy(dtype=float) - probabilities) ** 2))

    result = data[
        [
            "location_id",
            "location_name",
            "county_name",
            "state_code",
            "timestamp",
            "timestamp_local",
            "timestamp_utc",
            "wind_gust_mph",
            "predicted_wind_gust_mph",
            "high_wind_event",
            "high_wind_risk_score",
            "high_wind_event_predicted",
            "risk_tier",
            "model_version",
            "trained_at",
            "training_row_count",
            "positive_event_count",
            "threshold_used",
            "model_feature_list",
            "feature_medians_json",
            "feature_scales_json",
            "model_coefficients_json",
            "model_brier_score",
        ]
    ].drop_duplicates().reset_index(drop=True)

    result["high_wind_risk_score"] = pd.to_numeric(result["high_wind_risk_score"], errors="coerce")
    result["predicted_wind_gust_mph"] = pd.to_numeric(result["predicted_wind_gust_mph"], errors="coerce")
    result["high_wind_event"] = pd.to_numeric(result["high_wind_event"], errors="coerce").fillna(0).astype(int)
    result["high_wind_event_predicted"] = pd.to_numeric(result["high_wind_event_predicted"], errors="coerce").fillna(0).astype(int)
    result["training_row_count"] = pd.to_numeric(result["training_row_count"], errors="coerce").fillna(0).astype(int)
    result["positive_event_count"] = pd.to_numeric(result["positive_event_count"], errors="coerce").fillna(0).astype(int)
    result["threshold_used"] = pd.to_numeric(result["threshold_used"], errors="coerce")
    result["model_brier_score"] = pd.to_numeric(result["model_brier_score"], errors="coerce")
    return result