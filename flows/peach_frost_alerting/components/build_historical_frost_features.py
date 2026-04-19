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
        ordered[f"wind_mean_last_{window}h"] = ordered["wind_speed_mph"].shift(1).rolling(window=window, min_periods=1).mean()

    ordered["hours_below_freezing_last_12h"] = ordered["temperature_f"].shift(1).lt(32.0).rolling(window=12, min_periods=1).sum()
    ordered["hours_below_frost_threshold_last_12h"] = ordered["temperature_f"].shift(1).lt(28.0).rolling(window=12, min_periods=1).sum()
    ordered["warming_after_cold_flag"] = (
        ordered["temp_change_3h"].fillna(0.0).gt(8.0)
        & ordered["temp_min_last_12h"].fillna(ordered["temperature_f"]).lt(28.0)
    ).astype(int)
    return ordered


@transform(
    inputs=[ref("read_weather_history")],
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
    read_weather_history: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    data = read_weather_history.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)

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
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.sort_values(["location_id", "timestamp"])
    data["hour_of_day"] = data["timestamp"].dt.hour
    data["day_of_week"] = data["timestamp"].dt.dayofweek
    data["month"] = data["timestamp"].dt.month
    data["is_weekend"] = (data["day_of_week"] >= 5).astype(int)
    data["frost_event"] = data["temperature_f"].lt(28.0).astype(int)
    data["freeze_event"] = data["temperature_f"].lt(32.0).astype(int)
    data["dewpoint_depression_f"] = data["temperature_f"] - data["dew_point_f"]
    data["soil_temp_gap_f"] = data["temperature_f"] - data["soil_temperature_0cm_f"]

    data = data.groupby("location_id", group_keys=False).apply(_add_location_features)

    feature_fill_defaults = {
        "temp_change_1h": 0.0,
        "temp_change_3h": 0.0,
        "temp_mean_last_6h": data["temperature_f"],
        "temp_min_last_6h": data["temperature_f"],
        "temp_mean_last_12h": data["temperature_f"],
        "temp_min_last_12h": data["temperature_f"],
        "humidity_mean_last_6h": data["relative_humidity"],
        "humidity_mean_last_12h": data["relative_humidity"],
        "wind_mean_last_6h": data["wind_speed_mph"],
        "wind_mean_last_12h": data["wind_speed_mph"],
        "hours_below_freezing_last_12h": 0.0,
        "hours_below_frost_threshold_last_12h": 0.0,
    }
    for column, default_value in feature_fill_defaults.items():
        if isinstance(default_value, pd.Series):
            data[column] = data[column].fillna(default_value)
        else:
            data[column] = data[column].fillna(default_value)

    data["relative_humidity"] = data["relative_humidity"].fillna(data.groupby("location_id")["relative_humidity"].transform("median"))
    data["wind_speed_mph"] = data["wind_speed_mph"].fillna(data.groupby("location_id")["wind_speed_mph"].transform("median"))
    data["dew_point_f"] = data["dew_point_f"].fillna(data["temperature_f"])
    data["dewpoint_depression_f"] = data["dewpoint_depression_f"].fillna(0.0)
    data["soil_temp_gap_f"] = data["soil_temp_gap_f"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    data["precipitation_in"] = data["precipitation_in"].fillna(0.0)

    data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"Built historical frost feature dataset with {len(data)} rows")
    return data