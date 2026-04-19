WITH event_hours AS (
    SELECT
        location_id,
        location_name,
        county_name,
        year,
        CAST(timestamp AS DATE) AS frost_date,
        timestamp,
        temperature_f,
        CASE WHEN temperature_f < 28 THEN 1 ELSE 0 END AS frost_event_hour
    FROM {{ ref('read_weather_history_10y_march_may') }}
    WHERE temperature_f < 28
)
SELECT
    location_id,
    location_name,
    county_name,
    year,
    COUNT(*) AS frost_events_march_may,
    COUNT(DISTINCT frost_date) AS frost_days_march_may,
    MIN(timestamp) AS first_frost_timestamp,
    MAX(timestamp) AS last_frost_timestamp
FROM event_hours
GROUP BY
    location_id,
    location_name,
    county_name,
    year

{{ with_test("not_null", column="location_id") }}
{{ with_test("not_null", column="year") }}