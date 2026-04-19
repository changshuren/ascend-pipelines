from __future__ import annotations

import numpy as np
import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import ref, test, transform


ROLLING_WINDOWS = [6, 12]


def _add_location_features(group: pd.DataFrame) -> pd.DataFrame:
    ordered = group.sort_values("timestamp").copy()
    ordered["temp_change_1h"] = ordered["temperature_f"].diff()
    ordered["temp_change_3h"] = ordered["temperature_f"].diff(3)

    for window in ROLLING_WINDOWS:
        ordered[f"temp_mean_last_{window}h"] = ordered["temperature_f"].shift(1).rolling(window=window, min_periods=1).mean()
        ordered[f"temp_min_last_{window}h"] = ordered["temperature_f"].shift(1).rolling(window=window, min_periods=1).min()
        ordered[f"humidity_mean_last_{window}h"] = ordered["relative_humidity"].shift(1).rolling(window=window, min_periods=1).mean()
        ordered[f"wind_mean_last_{window}h"] = ordered["wind_speed_kmh"].shift(1).rolling(window=window, min_periods=1).mean()
        ordered[f"cloud_mean_last_{window}h"] = ordered["cloud_cover"].shift(1).rolling(window=window, min_periods=1).mean()

    ordered["hours_below_freezing_last_12h"] = ordered["temperature_f"].shift(1).lt(32.0).rolling(window=12, min_periods=1).sum()
    ordered["hours_below_frost_threshold_last_12h"] = ordered["temperature_f"].shift(1).lt(28.0).rolling(window=12, min_periods=1).sum()
    ordered["warming_after_cold_flag"] = (
        ordered["temp_change_3h"].fillna(0.0).gt(8.0)
        & ordered["temp_min_last_12h"].fillna(ordered["temperature_f"]).lt(28.0)
    ).astype(int)
    return ordered


@transform(
    inputs=[ref("read_weather_history_30d")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="timestamp"),
        test("not_null", column="temperature_f"),
        test("not_null", column="frost_event"),
    ],
)
def build_historical_frost_features(
    read_weather_history_30d: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    data = read_weather_history_30d.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)

    for column in ["temperature_f", "relative_humidity", "wind_speed_kmh", "cloud_cover"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.sort_values(["location_id", "timestamp"])
    data["hour_of_day"] = data["timestamp"].dt.hour
    data["day_of_week"] = data["timestamp"].dt.dayofweek
    data["month"] = data["timestamp"].dt.month
    data["is_weekend"] = (data["day_of_week"] >= 5).astype(int)
    data["frost_event"] = data["temperature_f"].lt(28.0).astype(int)
    data["freeze_event"] = data["temperature_f"].lt(32.0).astype(int)

    data = data.groupby("location_id", group_keys=False).apply(_add_location_features)

    fill_from_temperature = [
        "temp_mean_last_6h",
        "temp_min_last_6h",
        "temp_mean_last_12h",
        "temp_min_last_12h",
    ]
    for column in fill_from_temperature:
        data[column] = data[column].fillna(data["temperature_f"])

    for column in [
        "temp_change_1h",
        "temp_change_3h",
        "hours_below_freezing_last_12h",
        "hours_below_frost_threshold_last_12h",
    ]:
        data[column] = data[column].fillna(0.0)

    for column in [
        "humidity_mean_last_6h",
        "humidity_mean_last_12h",
        "relative_humidity",
    ]:
        data[column] = data[column].fillna(data.groupby("location_id")["relative_humidity"].transform("median")).fillna(0.0)

    for column in [
        "wind_mean_last_6h",
        "wind_mean_last_12h",
        "wind_speed_kmh",
        "cloud_mean_last_6h",
        "cloud_mean_last_12h",
        "cloud_cover",
    ]:
        data[column] = data[column].fillna(data.groupby("location_id")[column if column in data.columns else "wind_speed_kmh"].transform("median") if column in ["wind_speed_kmh", "cloud_cover"] else 0.0).fillna(0.0)

    data = data.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"Built Sangamon historical frost features with {len(data)} rows")
    return data