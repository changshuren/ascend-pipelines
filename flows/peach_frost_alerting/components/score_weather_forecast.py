from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.resources import ref, test, transform


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(value, -30.0, 30.0)))


def _add_location_features(group: pd.DataFrame) -> pd.DataFrame:
    ordered = group.sort_values("timestamp").copy()
    ordered["temp_change_1h"] = ordered["temperature_f"].diff()
    ordered["temp_change_3h"] = ordered["temperature_f"].diff(3)
    ordered["temp_mean_last_6h"] = ordered["temperature_f"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["temp_min_last_6h"] = ordered["temperature_f"].shift(1).rolling(window=6, min_periods=1).min()
    ordered["temp_mean_last_12h"] = ordered["temperature_f"].shift(1).rolling(window=12, min_periods=1).mean()
    ordered["temp_min_last_12h"] = ordered["temperature_f"].shift(1).rolling(window=12, min_periods=1).min()
    ordered["humidity_mean_last_6h"] = ordered["relative_humidity"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["humidity_mean_last_12h"] = ordered["relative_humidity"].shift(1).rolling(window=12, min_periods=1).mean()
    ordered["wind_mean_last_6h"] = ordered["wind_speed_mph"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["wind_mean_last_12h"] = ordered["wind_speed_mph"].shift(1).rolling(window=12, min_periods=1).mean()
    ordered["hours_below_freezing_last_12h"] = ordered["temperature_f"].shift(1).lt(32.0).rolling(window=12, min_periods=1).sum()
    ordered["hours_below_frost_threshold_last_12h"] = ordered["temperature_f"].shift(1).lt(28.0).rolling(window=12, min_periods=1).sum()
    ordered["warming_after_cold_flag"] = (
        ordered["temp_change_3h"].fillna(0.0).gt(8.0)
        & ordered["temp_min_last_12h"].fillna(ordered["temperature_f"]).lt(28.0)
    ).astype(int)
    return ordered


@transform(
    inputs=[
        ref("read_weather_forecast"),
        ref("build_frost_risk_model"),
    ],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="frost_risk_score"),
    ],
)
def score_weather_forecast(
    read_weather_forecast: pd.DataFrame,
    build_frost_risk_model: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    forecast = read_weather_forecast.copy()
    model = build_frost_risk_model.copy()

    forecast["timestamp"] = pd.to_datetime(forecast["timestamp"], utc=True)
    numeric_columns = [
        "temperature_f",
        "temperature_c",
        "relative_humidity",
        "wind_speed_mph",
        "dew_point_f",
        "dew_point_c",
        "precipitation_in",
        "soil_temperature_0cm_f",
    ]
    for column in numeric_columns:
        forecast[column] = pd.to_numeric(forecast[column], errors="coerce")

    forecast = forecast.sort_values(["location_id", "timestamp"])
    forecast["hour_of_day"] = forecast["timestamp"].dt.hour
    forecast["day_of_week"] = forecast["timestamp"].dt.dayofweek
    forecast["month"] = forecast["timestamp"].dt.month
    forecast["is_weekend"] = (forecast["day_of_week"] >= 5).astype(int)
    forecast["dewpoint_depression_f"] = forecast["temperature_f"] - forecast["dew_point_f"]
    forecast["soil_temp_gap_f"] = forecast["temperature_f"] - forecast["soil_temperature_0cm_f"]

    forecast = forecast.groupby("location_id", group_keys=False).apply(_add_location_features)

    forecast["dew_point_f"] = forecast["dew_point_f"].fillna(forecast["temperature_f"])
    forecast["dewpoint_depression_f"] = forecast["dewpoint_depression_f"].fillna(0.0)
    forecast["soil_temp_gap_f"] = forecast["soil_temp_gap_f"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    forecast["precipitation_in"] = forecast["precipitation_in"].fillna(0.0)
    for column in [
        "temp_change_1h",
        "temp_change_3h",
        "hours_below_freezing_last_12h",
        "hours_below_frost_threshold_last_12h",
    ]:
        forecast[column] = forecast[column].fillna(0.0)

    model_row = model.iloc[0]
    feature_names = json.loads(model_row["model_feature_list"])
    medians = json.loads(model_row["feature_medians_json"])
    scales = json.loads(model_row["feature_scales_json"])
    coefficients = json.loads(model_row["model_coefficients_json"])
    threshold = float(model_row["model_threshold"])

    for column in feature_names:
        forecast[column] = pd.to_numeric(forecast.get(column), errors="coerce").fillna(medians.get(column, 0.0))

    standardized = pd.DataFrame(
        {
            column: (forecast[column] - medians.get(column, 0.0)) / max(scales.get(column, 1.0), 1e-9)
            for column in feature_names
        }
    )
    base_temperature_signal = np.clip((28.0 - forecast["temperature_f"]) / 10.0, -2.0, 2.0)
    trend_signal = np.clip((28.0 - forecast["temp_min_last_12h"]) / 10.0, -2.0, 2.0)
    humidity_signal = np.clip((forecast["relative_humidity"] - 70.0) / 30.0, -1.5, 1.5)
    wind_signal = np.clip((forecast["wind_speed_mph"] - 5.0) / 15.0, -1.5, 1.5)
    learned_signal = sum(standardized[column] * coefficients.get(column, 0.0) for column in feature_names)
    logits = (
        coefficients.get("intercept", 0.0)
        + (coefficients.get("base_temperature_weight", 1.8) * base_temperature_signal)
        + (coefficients.get("trend_weight", 0.9) * trend_signal)
        + (coefficients.get("humidity_weight", 0.3) * humidity_signal)
        + (coefficients.get("wind_weight", 0.2) * wind_signal)
        + learned_signal
    )
    forecast["predicted_temp_f"] = forecast["temperature_f"]
    forecast["frost_risk_score"] = _sigmoid(np.asarray(logits, dtype=float))
    forecast["frost_event_predicted"] = (
        forecast["predicted_temp_f"].lt(28.0)
        | forecast["frost_risk_score"].ge(threshold)
    ).astype(bool)
    forecast["prediction_threshold"] = threshold
    forecast["risk_reason"] = np.where(
        forecast["predicted_temp_f"].lt(28.0),
        "predicted_temp_below_28f",
        np.where(forecast["frost_risk_score"].ge(threshold), "model_score_above_threshold", "low_risk"),
    )
    forecast["timestamp"] = forecast["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return forecast