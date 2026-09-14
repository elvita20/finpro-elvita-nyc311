CREATE OR REPLACE VIEW
`jcdeah-009.finpro_elvita_nyc311_dw.vw_dashboard_service_requests`
AS
SELECT
    unique_key,
    complaint_type,
    agency,
    agency_name,
    borough,
    status,
    created_at,
    closed_at,
    DATETIME_DIFF(closed_at, created_at, HOUR) AS resolution_hours,
    source_partition_date
FROM
    `jcdeah-009.finpro_elvita_nyc311_dw.fact_service_requests`
WHERE
    created_at IS NOT NULL
