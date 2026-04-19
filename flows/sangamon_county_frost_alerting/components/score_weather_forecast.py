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
    ordered["wind_mean_last_6h"] = ordered["wind_speed_kmh"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["wind_mean_last_12h"] = ordered["wind_speed_kmh"].shift(1).rolling(window=12, min_periods=1).mean()
    ordered["cloud_mean_last_6h"] = ordered["cloud_cover"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["cloud_mean_last_12h"] = ordered["cloud_cover"].shift(1).rolling(window=12, min_periods=1).mean()
    ordered["hours_below_freezing_last_12h"] = ordered["temperature_f"].shift(1).lt(32.0).rolling(window=12, min_periods=1).sum()
    ordered["hours_below_frost_threshold_last_12h"] = ordered["temperature_f"].shift(1).lt(28.0).rolling(window=12, min_periods=1).sum()
    ordered["warming_after_cold_flag"] = (
        ordered["temp_change_3h"].fillna(0.0).gt(8.0)
        & ordered["temp_min_last_12h"].fillna(ordered["temperature_f"]).lt(28.0)
    ).astype(int)
    return ordered


@transform(
    inputs=[ref("read_weather_forecast_3d"), ref("build_frost_risk_model")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="frost_risk_score"),
    ],
)
def score_weather_forecast(
    read_weather_forecast_3d: pd.DataFrame,
    build_frost_risk_model: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:

    # ------------------------------------------------------------------
    # ⭐ CRITICAL DEFENSIVE CHECKS ⭐
    # Prevents the "NoneType has no attribute op" error
    # ------------------------------------------------------------------
    if read_weather_forecast_3d is None or len(read_weather_forecast_3d) == 0:
        raise ValueError("read_weather_forecast_3d is empty or missing")

    if build_frost_risk_model is None or len(build_frost_risk_model) == 0:
        raise ValueError("build_frost_risk_model is empty or missing")

    forecast = read_weather_forecast_3d.copy()
    model = build_frost_risk_model.copy()

    # ------------------------------------------------------------------
    # ⭐ CRITICAL FIX ⭐
    # Convert timestamp to ISO string BEFORE any operations so Ascend
    # never infers datetime64 → SQL datetime (unsupported)
    # ------------------------------------------------------------------
    forecast["timestamp"] = (
        pd.to_datetime(forecast["timestamp"], utc=True)
          .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    # Convert numeric fields
    for column in ["temperature_f", "relative_humidity", "wind_speed_kmh", "cloud_cover", "precipitation_probability"]:
        forecast[column] = pd.to_numeric(forecast[column], errors="coerce")

    # Sort using ISO strings (safe)
    forecast = forecast.sort_values(["location_id", "timestamp"])

    # Extract time features
    ts = pd.to_datetime(forecast["timestamp"], utc=True)
    forecast["hour_of_day"] = ts.dt.hour
    forecast["day_of_week"] = ts.dt.dayofweek
    forecast["month"] = ts.dt.month
    forecast["is_weekend"] = (forecast["day_of_week"] >= 5).astype(int)

    # Add rolling features
    forecast = forecast.groupby("location_id", group_keys=False).apply(_add_location_features)

    # Fill missing values
    for column in [
        "temp_mean_last_6h",
        "temp_min_last_6h",
        "temp_mean_last_12h",
        "temp_min_last_12h",
    ]:
        forecast[column] = forecast[column].fillna(forecast["temperature_f"])

    for column in [
        "temp_change_1h",
        "temp_change_3h",
        "hours_below_freezing_last_12h",
        "hours_below_frost_threshold_last_12h",
        "precipitation_probability",
    ]:
        forecast[column] = forecast[column].fillna(0.0)

    for column in [
        "relative_humidity",
        "humidity_mean_last_6h",
        "humidity_mean_last_12h",
        "wind_speed_kmh",
        "wind_mean_last_6h",
        "wind_mean_last_12h",
        "cloud_cover",
        "cloud_mean_last_6h",
        "cloud_mean_last_12h",
    ]:
        forecast[column] = (
            forecast[column]
            .fillna(forecast.groupby("location_id")[column].transform("median"))
            .fillna(0.0)
        )

    # Load model metadata
    model_row = model.iloc[0]
    feature_names = json.loads(model_row["model_feature_list"])
    medians = json.loads(model_row["feature_medians_json"])
    scales = json.loads(model_row["feature_scales_json"])
    coefficients = json.loads(model_row["model_coefficients_json"])
    threshold = float(model_row["threshold_used"])

    # Standardize features
    for column in feature_names:
        forecast[column] = pd.to_numeric(forecast.get(column), errors="coerce").fillna(medians.get(column, 0.0))

    standardized = pd.DataFrame(
        {
            column: (forecast[column] - medians.get(column, 0.0)) / max(scales.get(column, 1.0), 1e-9)
            for column in feature_names
        }
    )

    # Model signals
    base_temperature_signal = np.clip((28.0 - forecast["temperature_f"]) / 10.0, -2.0, 2.0)
    trend_signal = np.clip((28.0 - forecast["temp_min_last_12h"]) / 10.0, -2.0, 2.0)
    humidity_signal = np.clip((forecast["relative_humidity"] - 70.0) / 30.0, -1.5, 1.5)
    wind_signal = np.clip((forecast["wind_speed_kmh"] - 10.0) / 20.0, -1.5, 1.5)
    learned_signal = sum(standardized[column] * coefficients.get(column, 0.0) for column in feature_names)

    logits = (
        coefficients.get("intercept", 0.0)
        + (coefficients.get("base_temperature_weight", 1.8) * base_temperature_signal)
        + (coefficients.get("trend_weight", 0.9) * trend_signal)
        + (coefficients.get("humidity_weight", 0.3) * humidity_signal)
        + (coefficients.get("wind_weight", 0.2) * wind_signal)
        + learned_signal
    )

    # Final outputs
    forecast["predicted_temp_f"] = forecast["temperature_f"]
    forecast["frost_risk_score"] = _sigmoid(np.asarray(logits, dtype=float))
    forecast["frost_event_predicted"] = (
        forecast["predicted_temp_f"].lt(28.0)
        | forecast["frost_risk_score"].ge(threshold)
    ).astype(bool)

    forecast["risk_tier"] = np.where(
        forecast["frost_risk_score"] >= 0.75,
        "high",
        np.where(forecast["frost_risk_score"] >= threshold, "medium", "low"),
    )

    # Compute day index
    ts2 = pd.to_datetime(forecast["timestamp"], utc=True)
    forecast["forecast_day_index"] = ((ts2 - ts2.min()).dt.total_seconds() // 86400).astype(int) + 1

    forecast["risk_reason"] = np.where(
        forecast["predicted_temp_f"].lt(28.0),
        "predicted_temp_below_28f",
        np.where(forecast["frost_risk_score"].ge(threshold), "model_score_above_threshold", "low_risk"),
    )

    return forecast
