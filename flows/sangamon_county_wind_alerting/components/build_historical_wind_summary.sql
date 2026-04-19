WITH flagged_history AS (
    SELECT
        location_id,
        location_name,
        year,
        month,
        CASE WHEN wind_gust_mph > 15 THEN 1 ELSE 0 END AS high_wind_event
    FROM {{ ref('read_weather_history_3y_march_nov') }}
),
monthly_summary AS (
    SELECT
        location_id,
        location_name,
        year,
        month,
        SUM(high_wind_event) AS monthly_high_wind_hours
    FROM flagged_history
    GROUP BY
        location_id,
        location_name,
        year,
        month
)
SELECT
    location_id,
    location_name,
    year,
    month,
    CAST(monthly_high_wind_hours AS FLOAT) AS monthly_high_wind_hours,
    CAST(SUM(monthly_high_wind_hours) OVER (PARTITION BY location_id, year) AS FLOAT) AS total_high_wind_hours_march_nov
FROM monthly_summary

{{ with_test("not_null", column="location_id") }}
{{ with_test("not_null", column="year") }}
{{ with_test("greater_than_or_equal", column="total_high_wind_hours_march_nov", value=0) }}