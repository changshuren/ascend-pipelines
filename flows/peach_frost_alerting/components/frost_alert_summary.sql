WITH risk_by_location AS (
    SELECT
        location_id,
        MIN(predicted_temp_f) AS predicted_min_temp_f,
        SUM(CASE WHEN frost_event_predicted = 1 THEN 1 ELSE 0 END) AS risky_hour_count
    FROM {{ ref('score_weather_forecast') }}
    GROUP BY
        location_id
),
alert_totals AS (
    SELECT
        COUNT(*) AS farmers_at_risk,
        COALESCE(SUM(risk_hour_count), 0) AS total_risk_hours,
        COALESCE(MAX(max_frost_risk_score), 0.0) AS max_frost_risk_score,
        COALESCE(COUNT(*), 0) AS alerts_sent_count
    FROM {{ ref('build_farmer_frost_alerts') }}
)
SELECT
    r.location_id,
    r.predicted_min_temp_f,
    r.risky_hour_count,
    a.farmers_at_risk,
    a.total_risk_hours,
    a.max_frost_risk_score,
    a.alerts_sent_count,
    CAST(SYSDATETIME() AS datetime2(6)) AS run_timestamp,
    CASE
        WHEN a.farmers_at_risk = 0 THEN 'no_frost_risk_detected'
        ELSE 'frost_risk_detected'
    END AS run_status
FROM risk_by_location r
CROSS JOIN alert_totals a;

{{ with_test("not_null", column="location_id") }}
{{ with_test("not_null", column="predicted_min_temp_f") }}