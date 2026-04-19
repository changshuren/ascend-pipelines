SELECT
    location_id,
    location_name,
    county_name,
    year,
    frost_events_march_may,
    frost_days_march_may,
    frost_days_march_may AS alerts_that_would_have_been_sent,
    frost_events_march_may AS potentially_saved_frost_events,
    first_frost_timestamp AS latest_event_timestamp
FROM {{ ref('build_retrospective_frost_events') }}

{{ with_test("not_null", column="location_id") }}
{{ with_test("not_null", column="year") }}