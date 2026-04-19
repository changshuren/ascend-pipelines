# Sangamon County Frost Alerting

Production-oriented frost alerting flow for Sangamon County, Illinois peach growers. The flow ingests live weather history and forecast data, scores frost risk for the next 3 days, joins those predictions to opted-in farmers, produces daily reports, and summarizes retrospective late spring frost exposure.

## Planned Components

### Live weather ingestion

- #component:read_weather_history_30d — pulls the last 30 days of hourly weather for hard-coded Sangamon County locations from the Open-Meteo archive API.
- #component:read_weather_forecast_3d — pulls the next 3 days of hourly forecast weather for the same locations from the Open-Meteo forecast API.
- #component:read_weather_history_10y_march_may — pulls 10 years of hourly March-May weather history for the same locations to support retrospective what-if analysis.

### Farmer registry

- #component:read_farmers_sangamon — farmer registry from @data/read_farmers_sangamon.csv.

### Modeling, alerting, and reporting

- #component:build_historical_frost_features — standardizes recent hourly history and labels frost events.
- #component:build_frost_risk_model — trains a simple interpretable frost-risk scorer from the historical features.
- #component:score_weather_forecast — scores hourly frost risk across the next 3 days.
- #component:build_farmer_frost_alerts — aggregates risky forecast windows into alert rows per opted-in farmer.
- #component:build_daily_weather_reports — creates 3-day weather report rows per farmer.
- #component:build_retrospective_frost_events — counts historical March-May frost events and frost days by location and year.
- #component:build_retrospective_impact — estimates how many historical frost events would have triggered alerts.
- #component:build_alerting_run_summary — dashboard-ready summary of current risk and retrospective impact.

### Notifications and operations

- #component:send_frost_alert_emails — scheduled 7:00 AM frost alert task.
- #component:send_daily_weather_report_emails — scheduled 12:00 PM daily report task.
- #automation:sangamon_county_frost_alerting — scheduled production automation and failure diagnostics.

## Live API payloads observed before implementation

### Open-Meteo archive

- Exact endpoint pattern to implement: `https://archive-api.open-meteo.com/v1/archive`
- Observed top-level keys included `latitude`, `longitude`, `generationtime_ms`, `utc_offset_seconds`, `timezone`, `timezone_abbreviation`, `elevation`, `hourly_units`, and `hourly`
- `hourly.time` is returned as hourly ISO8601 local-time strings
- Weather variables are returned as parallel arrays under `hourly`
- Observed response included requested hourly variables `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, and `cloud_cover`

### Open-Meteo forecast

- Exact endpoint pattern to implement: `https://api.open-meteo.com/v1/forecast`
- Observed payload shape matched the archive structure with `hourly_units` and `hourly`
- Forecast payload included `precipitation_probability` in the hourly object
- Observed forecast response returned hourly local timestamps for `America/Chicago`

## Notes

- Temperature is requested directly in Fahrenheit.
- Operational processing will normalize timestamps to UTC while preserving local-time context for reporting and email messaging.
- Locations are hard-coded within Sangamon County so `location_id` joins are deterministic for alerts, reports, and retrospective analysis.