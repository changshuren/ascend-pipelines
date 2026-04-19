{{
    config(
        type="task",
        dependencies=[ref('build_daily_weather_reports')]
    )
}}

SELECT
    farmer_id,
    email,
    'Daily Weather Report: 3-Day Outlook for Your Peach Trees' AS email_subject,
    CONCAT(
        'Hello ', farmer_name, ',', CHAR(10), CHAR(10),
        'Here is your 3-day weather outlook for ', location_name, ' in Sangamon County.', CHAR(10),
        three_day_temperature_summary, CHAR(10), CHAR(10),
        'Frost risk notes: ', frost_risk_notes, CHAR(10), CHAR(10),
        'When frost risk is present, consider blossom protection measures before overnight lows arrive.'
    ) AS email_body,
    CURRENT_TIMESTAMP AS generated_at
FROM {{ ref('build_daily_weather_reports') }}