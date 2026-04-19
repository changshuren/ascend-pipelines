WITH alert_metrics AS (
    SELECT
        COUNT(*) AS alerts_generated,
        COUNT(DISTINCT farmer_id) AS farmers_at_risk_next_3_days,
        COUNT(DISTINCT location_id) AS locations_with_risk,
        MIN(min_predicted_temp_f) AS min_predicted_temp_f_overall,
        MAX(max_frost_risk_score) AS max_frost_risk_score
    FROM {{ ref('build_farmer_frost_alerts') }}
),
report_metrics AS (
    SELECT
        COUNT(*) AS reports_generated
    FROM {{ ref('build_daily_weather_reports') }}
),
retrospective_ranked AS (
    SELECT
        county_name,
        location_name,
        potentially_saved_frost_events,
        ROW_NUMBER() OVER (
            PARTITION BY county_name
            ORDER BY potentially_saved_frost_events DESC, location_name
        ) AS location_rank
    FROM {{ ref('build_retrospective_impact') }}
),
retrospective_summary AS (
    SELECT
        county_name,
        STRING_AGG(
            CONCAT(
                location_name,
                ' (',
                CAST(potentially_saved_frost_events AS VARCHAR(32)),
                ' events)'
            ),
            ' | '
        ) AS location_summary
    FROM retrospective_ranked
    WHERE location_rank <= 5
    GROUP BY county_name
),
retrospective_metrics AS (
    SELECT
        MAX(location_summary) AS retrospective_high_impact_locations_summary
    FROM retrospective_summary
)
SELECT
    '{{ run_id }}' AS run_id,
    CAST(SYSDATETIME() AS datetime2(6)) AS run_timestamp,
    alert_metrics.farmers_at_risk_next_3_days,
    alert_metrics.alerts_generated,
    report_metrics.reports_generated,
    alert_metrics.locations_with_risk,
    alert_metrics.min_predicted_temp_f_overall,
    alert_metrics.max_frost_risk_score,
    retrospective_metrics.retrospective_high_impact_locations_summary,
    CASE
        WHEN alert_metrics.alerts_generated = 0 THEN 'no_frost_risk_detected'
        ELSE 'frost_risk_detected'
    END AS status_note
FROM alert_metrics
CROSS JOIN report_metrics
CROSS JOIN retrospective_metrics

{{ with_test("not_null", column="run_id") }}