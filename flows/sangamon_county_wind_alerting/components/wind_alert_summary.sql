WITH future_alerts AS (
    SELECT
        location_id,
        location_name,
        alert_timestamp,
        alert_timestamp_local,
        predicted_wind_gust_mph,
        high_wind_risk_score,
        risk_tier,
        COUNT(*) AS alerts_per_hour
    FROM {{ ref('build_wind_alerts') }}
    GROUP BY
        location_id,
        location_name,
        alert_timestamp,
        alert_timestamp_local,
        predicted_wind_gust_mph,
        high_wind_risk_score,
        risk_tier
),
historical_context AS (
    SELECT
        location_id,
        location_name,
        year,
        MAX(total_high_wind_hours_march_nov) AS total_high_wind_hours_march_nov
    FROM {{ ref('build_historical_wind_summary') }}
    GROUP BY
        location_id,
        location_name,
        year
)
SELECT
    f.location_id,
    f.location_name,
    f.alert_timestamp,
    f.alert_timestamp_local,
    CAST(f.predicted_wind_gust_mph AS FLOAT) AS predicted_wind_gust_mph,
    CAST(f.high_wind_risk_score AS FLOAT) AS high_wind_risk_score,
    f.risk_tier,
    CAST(f.alerts_per_hour AS FLOAT) AS alerts_per_hour,
    h.year,
    CAST(h.total_high_wind_hours_march_nov AS FLOAT) AS total_high_wind_hours_march_nov
FROM future_alerts f
LEFT JOIN historical_context h
    ON f.location_id = h.location_id

{{ with_test("not_null", column="location_id") }}
{{ with_test("not_null", column="alert_timestamp") }}
{{ with_test("greater_than_or_equal", column="alerts_per_hour", value=0) }}