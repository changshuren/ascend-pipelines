from __future__ import annotations

import pandas as pd

from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import ref, test, transform


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
    inputs=[ref("read_weather_history_30d")],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="timestamp"),
        test("not_null", column="wind_gust_mph"),
        test("not_null", column="high_wind_event"),
    ],
)
def build_historical_wind_features(
    read_weather_history_30d: pd.DataFrame,
    context: ComponentExecutionContext,
) -> pd.DataFrame:
    data = read_weather_history_30d.copy()
    data["timestamp_utc"] = pd.to_datetime(data["timestamp_utc"], utc=True)

    for column in ["temperature_f", "precipitation_in", "wind_speed_mph", "wind_gust_mph"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    before_drop = len(data)
    data = data.dropna(subset=["timestamp_utc", "location_id", "wind_gust_mph"]).copy()
    dropped_rows = before_drop - len(data)
    if dropped_rows > 0:
        log(f"Dropped {dropped_rows} historical weather rows due to missing critical fields")

    data = data.sort_values(["location_id", "timestamp_utc"]).reset_index(drop=True)
    data["hour_of_day"] = data["timestamp_utc"].dt.tz_convert("America/Chicago").dt.hour
    data["day_of_week"] = data["timestamp_utc"].dt.tz_convert("America/Chicago").dt.dayofweek
    data["month"] = data["timestamp_utc"].dt.tz_convert("America/Chicago").dt.month
    data["is_weekend"] = (data["day_of_week"] >= 5).astype(int)
    data["high_wind_event"] = data["wind_gust_mph"].gt(15.0).astype(int)
    data["risk_tier"] = pd.cut(
        data["wind_gust_mph"],
        bins=[-float("inf"), 15.0, 20.0, 25.0, float("inf")],
        labels=["low", "elevated", "high", "severe"],
        right=False,
    ).astype(str)

    data = data.groupby("location_id", group_keys=False).apply(_add_location_features)

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
        data[column] = data[column].fillna(data[base_column])

    zero_fill = [
        "precipitation_in_lag_6h_sum",
        "temperature_f_change_1h",
        "wind_gust_change_1h",
        "hours_above_15_mph_last_12h",
        "hours_above_20_mph_last_12h",
    ]
    for column in zero_fill:
        data[column] = data[column].fillna(0.0)

    result = data[
        [
            "location_id",
            "location_name",
            "county_name",
            "state_code",
            "latitude",
            "longitude",
            "timestamp",
            "timestamp_local",
            "timestamp_utc",
            "temperature_f",
            "precipitation_in",
            "wind_speed_mph",
            "wind_gust_mph",
            "hour_of_day",
            "day_of_week",
            "month",
            "is_weekend",
            "high_wind_event",
            "risk_tier",
            "wind_gust_mph_lag_1h",
            "wind_gust_mph_lag_3h_avg",
            "wind_gust_mph_lag_6h_max",
            "wind_speed_mph_lag_3h_avg",
            "wind_speed_mph_lag_6h_avg",
            "precipitation_in_lag_6h_sum",
            "temperature_f_lag_3h_avg",
            "temperature_f_change_1h",
            "wind_gust_change_1h",
            "hours_above_15_mph_last_12h",
            "hours_above_20_mph_last_12h",
        ]
    ].reset_index(drop=True)

    log(f"Prepared {len(result)} historical wind feature rows")
    return result