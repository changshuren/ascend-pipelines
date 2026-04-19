from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import read, test


SANGAMON_TIMEZONE = "America/Chicago"
ARCHIVE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_FIELDS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
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
        "location_id": "LOC_MECHANICSBURG",
        "location_name": "Mechanicsburg",
        "latitude": 39.8095,
        "longitude": -89.3945,
    },
]


def _request_json(params: dict[str, Any]) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = requests.get(ARCHIVE_ENDPOINT, params=params, timeout=60)
            if response.status_code in {429, 500, 502, 503, 504}:
                retry_after = response.headers.get("Retry-After")
                delay_seconds = float(retry_after) if retry_after else min(2 ** (attempt - 1), 30)
                if attempt == 5:
                    response.raise_for_status()
                log(f"Transient Open-Meteo archive error {response.status_code}; retrying in {delay_seconds} seconds")
                time.sleep(delay_seconds)
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object payload from Open-Meteo archive endpoint")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == 5:
                break
            delay_seconds = min(2 ** (attempt - 1), 30)
            log(f"Archive request attempt {attempt} failed: {exc}. Retrying in {delay_seconds} seconds")
            time.sleep(delay_seconds)

    raise RuntimeError("Open-Meteo archive request exhausted retries") from last_error


def _normalize_payload(location: dict[str, Any], payload: dict[str, Any]) -> pd.DataFrame:
    hourly = payload.get("hourly")
    hourly_units = payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(hourly_units, dict):
        raise ValueError("Open-Meteo archive response missing hourly payload or units")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError("Open-Meteo archive response missing hourly.time values")

    frame = pd.DataFrame({
        "timestamp_local": pd.to_datetime(times).tz_localize(
            ZoneInfo(SANGAMON_TIMEZONE),
            nonexistent="shift_forward",
            ambiguous="infer",
        )
    })

    for field_name in HOURLY_FIELDS:
        field_values = hourly.get(field_name)
        if not isinstance(field_values, list) or len(field_values) != len(frame):
            frame[field_name] = pd.Series([None] * len(frame), dtype="float")
        else:
            frame[field_name] = pd.to_numeric(field_values, errors="coerce")

    frame["timestamp_utc"] = frame["timestamp_local"].dt.tz_convert("UTC")
    frame["timestamp"] = frame["timestamp_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["timestamp_local"] = frame["timestamp_local"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    frame["location_id"] = location["location_id"]
    frame["location_name"] = location["location_name"]
    frame["county_name"] = "Sangamon"
    frame["state_code"] = "IL"
    frame["latitude"] = payload.get("latitude", location["latitude"])
    frame["longitude"] = payload.get("longitude", location["longitude"])
    frame["source_timezone"] = payload.get("timezone", SANGAMON_TIMEZONE)
    frame["timezone_abbreviation"] = payload.get("timezone_abbreviation")
    frame["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    frame["temperature_f"] = frame["temperature_2m"]
    frame["precipitation_in"] = frame["precipitation"]
    frame["wind_speed_mph"] = frame["wind_speed_10m"]
    frame["wind_gust_mph"] = frame["wind_gusts_10m"]
    frame["temperature_unit"] = hourly_units.get("temperature_2m")
    frame["precipitation_unit"] = hourly_units.get("precipitation")
    frame["wind_speed_unit"] = hourly_units.get("wind_speed_10m")
    frame["wind_gust_unit"] = hourly_units.get("wind_gusts_10m")
    frame["observed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    frame["source_url"] = ARCHIVE_ENDPOINT
    return frame[
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
            "source_timezone",
            "timezone_abbreviation",
            "utc_offset_seconds",
            "temperature_unit",
            "precipitation_unit",
            "wind_speed_unit",
            "wind_gust_unit",
            "observed_at",
            "source_url",
        ]
    ]


@read(
    strategy="full",
    on_schema_change="sync_all_columns",
    tests=[
        test("count_greater_than", count=0),
        test("not_null", column="location_id"),
        test("not_null", column="timestamp"),
        test("not_null", column="wind_gust_mph"),
    ],
)
def read_weather_history_30d(context: ComponentExecutionContext) -> pd.DataFrame:
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=29)
    frames: list[pd.DataFrame] = []

    for location in LOCATIONS:
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(HOURLY_FIELDS),
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "timezone": SANGAMON_TIMEZONE,
        }
        payload = _request_json(params)
        frame = _normalize_payload(location, payload)
        frames.append(frame)
        log(f"Fetched {len(frame)} 30-day weather rows for {location['location_id']}")

    result = pd.concat(frames, ignore_index=True)
    log(f"Prepared {len(result)} total 30-day hourly weather rows across {len(LOCATIONS)} locations")
    return result