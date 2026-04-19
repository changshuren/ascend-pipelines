# Sangamon County Wind Alerting

Operational wind alerting flow for commercial mowing and herbicide spraying companies in Sangamon County, Illinois. The flow ingests live hourly weather history and forecast data, predicts high-wind risk for the next 3 days, joins those predictions to opted-in subscribers, produces alert-ready rows, supports retrospective wind reporting, and will power a dashboard.

## Planned Components

### Live weather ingestion

- #component:read_weather_history_30d — pulls the last 30 days of hourly weather for hard-coded Sangamon County locations from the Open-Meteo archive API.
- #component:read_weather_forecast_3d — pulls the next 3 days of hourly forecast weather for the same locations from the Open-Meteo forecast API.
- #component:read_weather_history_3y_march_nov — pulls March-November hourly weather history for the last 3 years for the same locations.

### Subscriber registry

- #component:read_subscribers_sangamon — subscriber registry from @data/read_subscribers_sangamon.csv.

### Modeling, alerting, and reporting

- #component:build_historical_wind_features — standardizes recent hourly weather history and labels high-wind events.
- #component:build_high_wind_risk_model — trains a simple interpretable wind-risk scorer.
- #component:score_weather_forecast — scores hourly wind risk across the next 3 days.
- #component:build_wind_alerts — creates alert-ready rows for opted-in subscribers.
- #component:build_historical_wind_summary — summarizes March-November high-wind hours across the last 3 years.
- #component:wind_alert_summary — dashboard-ready summary of current risk, alerting activity, and historical context.

### Notifications and operations

- #component:send_wind_alert_emails — alert delivery task component.
- #automation:sangamon_county_wind_alerting — scheduled production automation and failure diagnostics.

## Live API payloads observed before implementation

### Open-Meteo archive

- Exact endpoint pattern to implement: `https://archive-api.open-meteo.com/v1/archive`
- Observed top-level keys included `latitude`, `longitude`, `generationtime_ms`, `utc_offset_seconds`, `timezone`, `timezone_abbreviation`, `elevation`, `hourly_units`, and `hourly`
- `hourly.time` is returned as hourly ISO8601 local-time strings
- Observed units included `temperature_2m: °F`, `precipitation: inch`, `wind_speed_10m: mp/h`, and `wind_gusts_10m: mp/h`
- Weather variables are returned as parallel arrays under `hourly`

### Open-Meteo forecast

- Exact endpoint pattern to implement: `https://api.open-meteo.com/v1/forecast`
- Observed payload shape matched the archive structure with `hourly_units` and `hourly`
- Forecast payload included `precipitation_probability` in the hourly object
- Observed forecast response returned hourly local timestamps for `America/Chicago`
- Observed forecast payload also included `wind_gusts_10m`, allowing direct gust-based alert logic

## Notes

- Wind speed and gust values are requested directly in mph-compatible units and will be normalized into explicit `wind_speed_mph` and `wind_gust_mph` columns.
- Operational processing will preserve local-time context for alerting while also materializing UTC timestamps for downstream consistency.
- Locations are hard-coded within Sangamon County so `location_id` joins are deterministic for alerting, modeling, and retrospective reporting.