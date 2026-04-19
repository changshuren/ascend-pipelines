from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import read, test


LOCATIONS = [
    {
        "location_id": "LOC_RALEIGH",
        "location_name": "Raleigh Peach Belt",
        "latitude": 35.7796,
        "longitude": -78.6382,
        "source_timezone": "America/New_York",
        "state_province": "North Carolina",
    },
    {
        "location_id": "LOC_ROANOKE",
        "location_name": "Roanoke Valley Orchard District",
        "latitude": 37.2709,
        "longitude": -79.9414,
        "source_timezone": "America/New_York",
        "state_province": "Virginia",
    },
    {
        "location_id": "LOC_ASHEVILLE",
        "location_name": "Asheville Mountain Orchard Zone",
        "latitude": 35.5951,
        "longitude": -82.5515,
        "source_timezone": "America/New_York",
        "state_province": "North Carolina",
    },
    {
        "location_id": "LOC_FRESNO",
        "location_name": "Fresno Central Valley Block",
        "latitude": 36.7378,
        "longitude": -119.7871,
        "source_timezone": "America/Los_Angeles",
        "state_province": "California",
    },
    {
        "location_id": "LOC_GRAND_JUNCTION",
        "location_name": "Grand Junction Western Slope",
        "latitude": 39.0639,
        "longitude": -108.5506,
        "source_timezone": "America/Denver",
        "state_province": "Colorado",
    },
]

WEATHER_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation_probability",
    "precipitation",
    "wind_speed_10m",
    "soil_temperature_0cm",
]

ENDPOINT = "https://api.open-meteo.com/v1/forecast"
NORMALIZED_TIMEZONE = "UTC"


def _request_json(url: str, params: dict[str, Any], timeout: int = 60) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == 5:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(2 ** (attempt - 1), 30)
                time.sleep(delay)
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object payload from Open-Meteo forecast endpoint")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == 5:
                break
            time.sleep(min(2 ** (attempt - 1), 30))

    raise RuntimeError("Open-Meteo forecast request exhausted retries") from last_error


def _normalize_frame(location: dict[str, Any], payload: dict[str, Any]) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("Open-Meteo forecast response missing hourly object")

    units = payload.get("hourly_units")
    if not isinstance(units, dict):
        raise ValueError("Open-Meteo forecast response missing hourly_units object")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError("Open-Meteo forecast response missing hourly.time values")

    source_ts = pd.to_datetime(times)
    frame = pd.DataFrame(
        {
            "timestamp": source_ts.tz_localize(location["source_timezone"]),
        }
    )
    frame["source_timestamp"] = source_ts.strftime("%Y-%m-%dT%H:%M:%S")

    for variable in WEATHER_VARS:
        values = hourly.get(variable)
        if isinstance(values, list):
            frame[variable] = pd.to_numeric(values, errors="coerce")
        else:
            frame[variable] = None

    frame["location_id"] = location["location_id"]
    frame["location_name"] = location["location_name"]
    frame["latitude"] = location["latitude"]
    frame["longitude"] = location["longitude"]
    frame["state_province"] = location["state_province"]
    frame["source_timezone"] = payload.get("timezone")
    frame["normalized_timezone"] = NORMALIZED_TIMEZONE
    frame["timezone_abbreviation"] = payload.get("timezone_abbreviation")
    frame["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    frame["temperature_unit"] = units.get("temperature_2m")
    frame["wind_speed_unit"] = units.get("wind_speed_10m")
    frame["precipitation_unit"] = units.get("precipitation")
    frame["soil_temperature_unit"] = units.get("soil_temperature_0cm")
    frame["temperature_f"] = frame["temperature_2m"]
    frame["temperature_c"] = ((frame["temperature_f"] - 32.0) * 5.0) / 9.0
    frame["dew_point_f"] = frame["dew_point_2m"]
    frame["dew_point_c"] = ((frame["dew_point_f"] - 32.0) * 5.0) / 9.0
    frame["relative_humidity"] = frame["relative_humidity_2m"]
    frame["wind_speed_mph"] = frame["wind_speed_10m"]
    frame["precipitation_probability_pct"] = frame["precipitation_probability"]
    frame["precipitation_in"] = frame["precipitation"]
    frame["soil_temperature_0cm_f"] = frame["soil_temperature_0cm"]
    frame["timestamp"] = frame["timestamp"].dt.tz_convert(NORMALIZED_TIMEZONE).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["source_url"] = ENDPOINT
    return frame


@read(
    strategy="full",
    on_schema_change="sync_all_columns",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="timestamp"),
        test("not_null", column="temperature_f"),
    ],
)
def read_weather_forecast(context: ComponentExecutionContext) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    for location in LOCATIONS:
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": ",".join(WEATHER_VARS),
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "forecast_days": 3,
            "timezone": location["source_timezone"],
        }
        payload = _request_json(ENDPOINT, params)
        frame = _normalize_frame(location, payload)
        rows.append(frame)
        log(f"Fetched {len(frame)} hourly forecast weather rows for {location['location_id']}")

    return pd.concat(rows, ignore_index=True)