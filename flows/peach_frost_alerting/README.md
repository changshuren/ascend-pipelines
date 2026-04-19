# Peach Frost Alerting

Production-style frost alerting flow for peach growers in North America. The flow learns from the last 30 days of live hourly weather, predicts frost risk for the next 3 days, joins risky windows to opted-in farmers, and sends alert notifications.

## Planned Components

### Live weather ingestion

- #component:read_weather_history — pulls the last 30 days of hourly weather for fixed farm locations from the Open-Meteo archive API.
- #component:read_weather_forecast — pulls the next 3 days of hourly forecast weather for the same farm locations from the Open-Meteo forecast API.

### Local registry data

- #component:read_farmers — farmer registry from @data/read_farmers.csv.

### Modeling and alerting

- #component:build_historical_frost_features — cleans history, labels frost events, and builds model features.
- #component:build_frost_risk_model — trains an interpretable frost-risk model from the historical feature set.
- #component:score_weather_forecast — applies the learned model to forecast hours.
- #component:build_farmer_frost_alerts — aggregates risky forecast windows to farmer alert rows.
- #component:frost_alert_summary — run-level summary output for observability and dashboarding.

### Notifications and operations

- Email notification task component — sends frost alert emails for risky farmer/location windows.
- Scheduled automation — refreshes data and re-evaluates risk every 3 hours.
- Failure-monitoring automation — triggers Otto diagnostics when scheduled runs fail.

## Live API payloads observed before implementation

### Open-Meteo archive

- Exact endpoint pattern to implement: `https://archive-api.open-meteo.com/v1/archive`
- Observed top-level keys include `latitude`, `longitude`, `generationtime_ms`, `utc_offset_seconds`, `timezone`, `timezone_abbreviation`, `elevation`, `hourly_units`, and `hourly`
- `hourly.time` is returned as hourly ISO8601 local-time strings
- Weather variables are parallel arrays under `hourly`
- Observed archive payload returned `soil_temperature_0cm` with unit `undefined` and null-valued hourly data for the sampled request, so downstream logic will treat this field as optional

### Open-Meteo forecast

- Exact endpoint pattern to implement: `https://api.open-meteo.com/v1/forecast`
- Observed payload shape matches the archive pattern with `hourly_units` and `hourly`
- Forecast payload includes `precipitation_probability`
- Forecast payload returned `soil_temperature_0cm` with populated Fahrenheit values for the sampled request

## Notes

- Temperature will be requested directly in Fahrenheit and normalized to UTC for downstream processing while preserving source timezone fields for auditability.
- The initial implementation uses hard-coded farm coordinates so farmer alerts can be joined cleanly on `location_id`.