WITH risky_hours AS (
    SELECT
        f.farmer_id,
        f.farmer_name,
        f.email,
        f.location_id,
        s.location_name,

        -- timestamp is already VARCHAR from score_weather_forecast
        s.timestamp AS timestamp,

        s.predicted_temp_f,
        s.frost_risk_score,
        s.frost_event_predicted,
        s.risk_tier
    FROM {{ ref('score_weather_forecast') }} AS s
    INNER JOIN {{ ref('read_farmers_sangamon') }} AS f
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
        location_name,

        MIN(timestamp) AS first_risk_timestamp,
        MIN(timestamp) AS risk_window_start,
        MAX(timestamp) AS risk_window_end,

        MIN(predicted_temp_f) AS min_predicted_temp_f,
        MAX(frost_risk_score) AS max_frost_risk_score,
        COUNT(*) AS hours_below_28f
    FROM risky_hours
    GROUP BY
        farmer_id,
        farmer_name,
        email,
        location_id,
        location_name
)

SELECT
    farmer_id,
    farmer_name,
    email,
    location_id,
    location_name,

    first_risk_timestamp,

    CONCAT(
        LEFT(risk_window_start, 19),
        ' UTC to ',
        LEFT(risk_window_end, 19),
        ' UTC'
    ) AS risk_window_summary,

    min_predicted_temp_f,
    max_frost_risk_score,
    hours_below_28f,

    CASE
        WHEN max_frost_risk_score >= 0.75 OR min_predicted_temp_f < 25 THEN 'high'
        WHEN max_frost_risk_score >= 0.4 OR min_predicted_temp_f < 28 THEN 'medium'
        ELSE 'low'
    END AS risk_tier,

    'predicted_temp_below_28f_or_model_score' AS alert_reason,

    CAST(SYSDATETIME() AS datetime2(6)) AS run_timestamp

FROM aggregated;

{{ with_test("not_null", column="farmer_id") }}
{{ with_test("not_null", column="email") }}
