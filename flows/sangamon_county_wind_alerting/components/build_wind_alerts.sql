SELECT
    s.subscriber_id,
    s.company_name,
    s.email,
    s.location_id,
    f.location_name,
    CAST(f.timestamp AS datetime2(6)) AS alert_timestamp,
    CAST(f.timestamp_local AS datetime2(6)) AS alert_timestamp_local,
    CAST(f.predicted_wind_gust_mph AS FLOAT) AS predicted_wind_gust_mph,
    CAST(f.high_wind_risk_score AS FLOAT) AS high_wind_risk_score,
    f.risk_tier,
    'Forecast wind gusts exceed 15 mph; herbicide spraying may violate label limits and increase drift risk.' AS advisory_message,
    CAST(SYSDATETIME() AS datetime2(6)) AS generated_at
FROM {{ ref('score_weather_forecast') }} AS f
INNER JOIN {{ ref('read_subscribers_sangamon') }} AS s
    ON f.location_id = s.location_id
WHERE
    s.alert_opt_in = 1
    AND (
        f.predicted_wind_gust_mph > 15
        OR f.high_wind_event_predicted = 1
    );

{{ with_test("not_null", column="subscriber_id") }}
{{ with_test("not_null", column="email") }}
{{ with_test("not_null", column="alert_timestamp") }}
{{ with_test("not_null", column="predicted_wind_gust_mph") }}