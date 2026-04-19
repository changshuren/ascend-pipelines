WITH risky_hours AS (
    SELECT
        f.farmer_id,
        f.farmer_name,
        f.email,
        f.location_id,
        CAST(s.timestamp AS datetime2(6)) AS timestamp,
        s.predicted_temp_f,
        s.frost_risk_score,
        s.frost_event_predicted
    FROM {{ ref('score_weather_forecast') }} AS s
    INNER JOIN {{ ref('read_farmers') }} AS f
        ON s.location_id = f.location_id
    WHERE
        f.alert_opt_in = 1
        AND (
            s.predicted_temp_f < 28
            OR s.frost_event_predicted = 1
        )
),
aggregated AS (
    SELECT
        farmer_id,
        farmer_name,
        email,
        location_id,
        MIN(timestamp) AS first_risk_timestamp,
        MIN(timestamp) AS risk_window_start,
        MAX(timestamp) AS risk_window_end,
        MAX(frost_risk_score) AS max_frost_risk_score,
        MIN(predicted_temp_f) AS expected_min_temp_f,
        COUNT(*) AS risk_hour_count
    FROM risky_hours
    GROUP BY
        farmer_id,
        farmer_name,
        email,
        location_id
)
SELECT
    farmer_id,
    farmer_name,
    email,
    location_id,
    CAST(first_risk_timestamp AS datetime2(6)) AS first_risk_timestamp,
    CONCAT(
        CONVERT(VARCHAR(19), CAST(risk_window_start AS datetime2(6)), 120),
        ' UTC to ',
        CONVERT(VARCHAR(19), CAST(risk_window_end AS datetime2(6)), 120),
        ' UTC'
    ) AS risk_window_summary,
    max_frost_risk_score,
    expected_min_temp_f,
    risk_hour_count,
    CAST(SYSDATETIME() AS datetime2(6)) AS run_timestamp
FROM aggregated;

{{ with_test("not_null", column="farmer_id") }}
{{ with_test("not_null", column="email") }}