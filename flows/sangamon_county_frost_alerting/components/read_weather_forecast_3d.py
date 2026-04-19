from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import read, test


SANGAMON_TIMEZONE = "America/Chicago"
NORMALIZED_TIMEZONE = "UTC"
FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "cloud_cover",
    "precipitation_probability",
]
LOCATIONS = [
    {
        "location_id": "LOC_SPRINGFIELD",
        "location_name": "Springfield",
        "latitude": 39.7817,
        "longitude": -89.6501,
    },
    {
        "location_id": "LOC_CHATHAM",
        "location_name": "Chatham",
        "latitude": 39.6762,
        "longitude": -89.7045,
    },
    {
        "location_id": "LOC_ROCHESTER",
        "location_name": "Rochester",
        "latitude": 39.7492,
        "longitude": -89.5318,
    },
    {
        "location_id": "LOC_SHERMAN",
        "location_name": "Sherman",
        "latitude": 39.8937,
        "longitude": -89.6040,
    },
    {
        "location_id": "LOC_PAWNEE",
        "location_name": "Pawnee",
        "latitude": 39.5917,
        "longitude": -89.5801,
    },
]


def _request_json(params: dict[str, object]) -> dict[str, object]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = requests.get(FORECAST_ENDPOINT, params=params, timeout=60)
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == 5:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay_seconds = float(retry_after) if retry_after else min(2 ** (attempt - 1), 30)
                time.sleep(delay_seconds)
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


def _normalize_payload(location: dict[str, object], payload: dict[str, object]) -> pd.DataFrame:
    hourly = payload.get("hourly")
    hourly_units = payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(hourly_units, dict):
        raise ValueError("Open-Meteo forecast response missing hourly payload or units")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError("Open-Meteo forecast response missing hourly.time values")

    source_timestamps = pd.to_datetime(times)
    localized_timestamps = source_timestamps.tz_localize(ZoneInfo(SANGAMON_TIMEZONE), nonexistent="shift_forward", ambiguous="infer")
    frame = pd.DataFrame({"timestamp_local": localized_timestamps})

    for field_name in HOURLY_FIELDS:
        field_values = hourly.get(field_name)
        frame[field_name] = pd.to_numeric(field_values, errors="coerce") if isinstance(field_values, list) else None

    frame["location_id"] = location["location_id"]
    frame["location_name"] = location["location_name"]
    frame["county_name"] = "Sangamon"
    frame["state_code"] = "IL"
    frame["latitude"] = payload.get("latitude", location["latitude"])
    frame["longitude"] = payload.get("longitude", location["longitude"])
    frame["source_timezone"] = payload.get("timezone", SANGAMON_TIMEZONE)
    frame["timezone_abbreviation"] = payload.get("timezone_abbreviation")
    frame["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    frame["normalized_timezone"] = NORMALIZED_TIMEZONE
    frame["temperature_unit"] = hourly_units.get("temperature_2m")
    frame["wind_speed_unit"] = hourly_units.get("wind_speed_10m")
    frame["cloud_cover_unit"] = hourly_units.get("cloud_cover")
    frame["precipitation_probability_unit"] = hourly_units.get("precipitation_probability")
    frame["temperature_f"] = frame["temperature_2m"]
    frame["relative_humidity"] = frame["relative_humidity_2m"]
    frame["wind_speed_kmh"] = frame["wind_speed_10m"]
    frame["forecast_generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["timestamp_utc"] = frame["timestamp_local"].dt.tz_convert(NORMALIZED_TIMEZONE)
    frame["timestamp"] = frame["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["timestamp_local"] = frame["timestamp_local"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    frame["source_url"] = FORECAST_ENDPOINT
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
def read_weather_forecast_3d(context: ComponentExecutionContext) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for location in LOCATIONS:
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "forecast_days": 3,
            "hourly": ",".join(HOURLY_FIELDS),
            "temperature_unit": "fahrenheit",
            "timezone": SANGAMON_TIMEZONE,
        }
        payload = _request_json(params)
        frame = _normalize_payload(location, payload)
        frames.append(frame)
        log(f"Fetched {len(frame)} 3-day forecast rows for {location['location_id']}")

    return pd.concat(frames, ignore_index=True)