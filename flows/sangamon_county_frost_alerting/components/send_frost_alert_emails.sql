{{
    config(
        type="task",
        dependencies=[ref('build_farmer_frost_alerts')]
    )
}}

SELECT
    farmer_id,
    email,
    CONCAT(
        'Frost Alert: Peach Tree Risk in Sangamon County on ',
        CONVERT(NVARCHAR(19), CAST(first_risk_timestamp AS DATETIME2), 120),
        ' UTC'
    ) AS email_subject,
    CONCAT(
        'Hello ', farmer_name, ',', CHAR(10), CHAR(10),
        'A late spring frost risk is forecast for ', location_name, ' in Sangamon County.', CHAR(10),
        'Risk window: ', risk_window_summary, CHAR(10),
        'Expected minimum temperature: ', CAST(ROUND(min_predicted_temp_f, 1) AS NVARCHAR(32)), ' F', CHAR(10),
        'Maximum frost risk score: ', CAST(ROUND(max_frost_risk_score, 3) AS NVARCHAR(32)), CHAR(10), CHAR(10),
        'Late spring frost below 28F can damage peach buds, blossoms, and young fruit.', CHAR(10),
        'Protective steps may include row covers, wind machines, irrigation strategies, or delaying sensitive orchard activity where feasible.', CHAR(10), CHAR(10),
        'This alert was generated automatically from the latest forecast pipeline run.'
    ) AS email_body,
    CURRENT_TIMESTAMP AS generated_at
FROM {{ ref('build_farmer_frost_alerts') }}