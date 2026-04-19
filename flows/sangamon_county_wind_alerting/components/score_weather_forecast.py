from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.resources import ref, test, transform


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(value, -30.0, 30.0)))


def _add_location_features(group: pd.DataFrame) -> pd.DataFrame:
    ordered = group.sort_values("timestamp_utc").copy()
    ordered["wind_gust_mph_lag_1h"] = ordered["wind_gust_mph"].shift(1)
    ordered["wind_gust_mph_lag_3h_avg"] = ordered["wind_gust_mph"].shift(1).rolling(window=3, min_periods=1).mean()
    ordered["wind_gust_mph_lag_6h_max"] = ordered["wind_gust_mph"].shift(1).rolling(window=6, min_periods=1).max()
    ordered["wind_speed_mph_lag_3h_avg"] = ordered["wind_speed_mph"].shift(1).rolling(window=3, min_periods=1).mean()
    ordered["wind_speed_mph_lag_6h_avg"] = ordered["wind_speed_mph"].shift(1).rolling(window=6, min_periods=1).mean()
    ordered["precipitation_in_lag_6h_sum"] = ordered["precipitation_in"].shift(1).rolling(window=6, min_periods=1).sum()
    ordered["temperature_f_lag_3h_avg"] = ordered["temperature_f"].shift(1).rolling(window=3, min_periods=1).mean()
    ordered["temperature_f_change_1h"] = ordered["temperature_f"].diff()
    ordered["wind_gust_change_1h"] = ordered["wind_gust_mph"].diff()
    ordered["hours_above_15_mph_last_12h"] = ordered["wind_gust_mph"].shift(1).gt(15.0).rolling(window=12, min_periods=1).sum()
    ordered["hours_above_20_mph_last_12h"] = ordered["wind_gust_mph"].shift(1).gt(20.0).rolling(window=12, min_periods=1).sum()
    return ordered


@transform(
    inputs=[ref("read_weather_forecast_3d"), ref("build_high_wind_risk_model")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="predicted_wind_gust_mph"),
        test("not_null", column="high_wind_risk_score"),
    ],
)
def score_weather_forecast(
    read_weather_forecast_3d: pd.DataFrame,
    build_high_wind_risk_model: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    forecast = read_weather_forecast_3d.copy()
    model = build_high_wind_risk_model.copy()

    forecast["timestamp_utc"] = pd.to_datetime(forecast["timestamp_utc"], utc=True)
    for column in ["temperature_f", "precipitation_probability_pct", "precipitation_in", "wind_speed_mph", "wind_gust_mph"]:
        forecast[column] = pd.to_numeric(forecast[column], errors="coerce")

    forecast = forecast.sort_values(["location_id", "timestamp_utc"]).reset_index(drop=True)
    forecast["hour_of_day"] = forecast["timestamp_utc"].dt.tz_convert("America/Chicago").dt.hour
    forecast["day_of_week"] = forecast["timestamp_utc"].dt.tz_convert("America/Chicago").dt.dayofweek
    forecast["month"] = forecast["timestamp_utc"].dt.tz_convert("America/Chicago").dt.month
    forecast["is_weekend"] = (forecast["day_of_week"] >= 5).astype(int)
    forecast = forecast.groupby("location_id", group_keys=False).apply(_add_location_features)

    fill_from_current = [
        "wind_gust_mph_lag_1h",
        "wind_gust_mph_lag_3h_avg",
        "wind_gust_mph_lag_6h_max",
        "wind_speed_mph_lag_3h_avg",
        "wind_speed_mph_lag_6h_avg",
        "temperature_f_lag_3h_avg",
    ]
    for column in fill_from_current:
        base_column = "wind_gust_mph" if "gust" in column else "wind_speed_mph" if "wind_speed" in column else "temperature_f"
        forecast[column] = forecast[column].fillna(forecast[base_column])

    zero_fill = [
        "precipitation_in_lag_6h_sum",
        "temperature_f_change_1h",
        "wind_gust_change_1h",
        "hours_above_15_mph_last_12h",
        "hours_above_20_mph_last_12h",
        "precipitation_probability_pct",
    ]
    for column in zero_fill:
        forecast[column] = forecast[column].fillna(0.0)

    model_row = model.iloc[0]
    feature_names = json.loads(model_row["model_feature_list"])
    medians = json.loads(model_row["feature_medians_json"])
    scales = json.loads(model_row["feature_scales_json"])
    coefficients = json.loads(model_row["model_coefficients_json"])
    threshold = float(model_row["threshold_used"])

    standardized = pd.DataFrame(
        {
            column: (pd.to_numeric(forecast[column], errors="coerce").fillna(medians.get(column, 0.0)) - medians.get(column, 0.0))
            / max(scales.get(column, 1.0), 1e-9)
            for column in feature_names
        }
    )

    base_gust_signal = np.clip((forecast["wind_gust_mph_lag_3h_avg"] - 15.0) / 10.0, -2.0, 2.0)
    max_gust_signal = np.clip((forecast["wind_gust_mph_lag_6h_max"] - 15.0) / 10.0, -2.0, 2.0)
    sustained_signal = np.clip((forecast["hours_above_15_mph_last_12h"] - 2.0) / 4.0, -2.0, 2.0)
    learned_signal = sum(standardized[column] * coefficients.get(column, 0.0) for column in feature_names)

    logits = (
        coefficients.get("intercept", 0.0)
        + (coefficients.get("base_gust_weight", 1.5) * base_gust_signal)
        + (coefficients.get("max_gust_weight", 1.0) * max_gust_signal)
        + (coefficients.get("sustained_weight", 0.7) * sustained_signal)
        + learned_signal
    )

    forecast["predicted_wind_gust_mph"] = (
        0.45 * forecast["wind_gust_mph_lag_1h"]
        + 0.35 * forecast["wind_gust_mph_lag_3h_avg"]
        + 0.20 * forecast["wind_gust_mph_lag_6h_max"]
    ).round(3)
    forecast["high_wind_risk_score"] = _sigmoid(np.asarray(logits, dtype=float))
    forecast["high_wind_event_predicted"] = (
        forecast["predicted_wind_gust_mph"].gt(15.0)
        | forecast["high_wind_risk_score"].ge(threshold)
    ).astype(int)
    forecast["risk_tier"] = np.where(
        forecast["predicted_wind_gust_mph"] >= 25.0,
        "severe",
        np.where(
            forecast["predicted_wind_gust_mph"] >= 20.0,
            "high",
            np.where(forecast["predicted_wind_gust_mph"] >= 15.0, "elevated", "low"),
        ),
    )
    forecast["risk_reason"] = np.where(
        forecast["predicted_wind_gust_mph"] > 15.0,
        "predicted_gust_above_threshold",
        np.where(forecast["high_wind_risk_score"] >= threshold, "model_score_above_threshold", "low_risk"),
    )
    forecast["forecast_day_index"] = ((forecast["timestamp_utc"] - forecast["timestamp_utc"].min()).dt.total_seconds() // 86400).astype(int) + 1

    result = forecast[
        [
            "location_id",
            "location_name",
            "county_name",
            "state_code",
            "timestamp",
            "timestamp_local",
            "timestamp_utc",
            "temperature_f",
            "precipitation_probability_pct",
            "precipitation_in",
            "wind_speed_mph",
            "wind_gust_mph",
            "predicted_wind_gust_mph",
            "high_wind_risk_score",
            "high_wind_event_predicted",
            "risk_tier",
            "risk_reason",
            "forecast_day_index",
        ]
    ].reset_index(drop=True)

    for column in [
        "temperature_f",
        "precipitation_probability_pct",
        "precipitation_in",
        "wind_speed_mph",
        "wind_gust_mph",
        "predicted_wind_gust_mph",
        "high_wind_risk_score",
        "forecast_day_index",
    ]:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    result["high_wind_event_predicted"] = pd.to_numeric(result["high_wind_event_predicted"], errors="coerce").fillna(0).astype(int)
    return result