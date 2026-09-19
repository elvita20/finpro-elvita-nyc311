-- ============================================================
-- dim_date
-- Diadaptasi dari struktur atribut: https://github.com/jonsunderland/dim_date
-- Diporting ke BigQuery native date functions (bukan terjemahan
-- baris-per-baris dari Python) supaya lebih idiomatis dan efisien
-- dijalankan langsung sebagai satu query di BigQuery.
--
-- Join key ke fact table: dt_y_m_d (DATE) <-> fact_service_requests.source_partition_date
-- ============================================================

CREATE OR REPLACE TABLE
`jcdeah-009.finpro_elvita_nyc311_dw.dim_date`
AS

WITH base_dates AS
(
    SELECT d AS dt_y_m_d
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2030-12-31')
    ) AS d
),

enriched AS
(
    SELECT
        dt_y_m_d,

        EXTRACT(YEAR FROM dt_y_m_d) AS dt_year,
        EXTRACT(MONTH FROM dt_y_m_d) AS dt_month,
        EXTRACT(DAY FROM dt_y_m_d) AS dt_day,

        IF(
            MOD(EXTRACT(YEAR FROM dt_y_m_d), 4) = 0
            AND (
                MOD(EXTRACT(YEAR FROM dt_y_m_d), 100) != 0
                OR MOD(EXTRACT(YEAR FROM dt_y_m_d), 400) = 0
            ),
            366, 365
        ) AS dt_days_in_year,

        EXTRACT(DAYOFYEAR FROM dt_y_m_d) AS dt_day_of_year,

        DATE(EXTRACT(YEAR FROM dt_y_m_d), 1, 1) AS dt_year_start_dt,
        DATE(EXTRACT(YEAR FROM dt_y_m_d), 12, 31) AS dt_year_end_dt,

        FORMAT_DATE('%Y%m', dt_y_m_d) AS dt_year_month,

        DATE_TRUNC(dt_y_m_d, MONTH) AS dt_month_start_dt,
        LAST_DAY(dt_y_m_d, MONTH) AS dt_month_end_dt,
        EXTRACT(DAY FROM LAST_DAY(dt_y_m_d, MONTH)) AS dt_days_in_month,

        FORMAT_DATE('%B', dt_y_m_d) AS dt_month_name,
        FORMAT_DATE('%b', dt_y_m_d) AS dt_month_name_short,

        (EXTRACT(DAYOFWEEK FROM dt_y_m_d) - 1) AS dt_iso_weekday,
        FORMAT_DATE('%A', dt_y_m_d) AS dt_iso_dow_full,
        FORMAT_DATE('%a', dt_y_m_d) AS dt_iso_dow_short,

        IF((EXTRACT(DAYOFWEEK FROM dt_y_m_d) - 1) IN (0, 6), 0, 1) AS dt_is_weekday,
        IF((EXTRACT(DAYOFWEEK FROM dt_y_m_d) - 1) IN (0, 6), 1, 0) AS dt_is_weekend,

        DATE_TRUNC(dt_y_m_d, WEEK(MONDAY)) AS dt_week_start_dt,
        DATE_ADD(DATE_TRUNC(dt_y_m_d, WEEK(MONDAY)), INTERVAL 6 DAY) AS dt_week_end_dt,

        CONCAT(FORMAT_DATE('%G', dt_y_m_d), 'W', FORMAT_DATE('%V', dt_y_m_d)) AS dt_iso_weeknumber
    FROM base_dates
),

with_pct AS
(
    SELECT
        *,
        (dt_days_in_year - dt_day_of_year) AS dt_days_left_in_year,
        ROUND(dt_day_of_year / dt_days_in_year, 2) AS dt_pct_of_year
    FROM enriched
),

with_radians AS
(
    SELECT
        *,
        (
            (ACOS(-1) / 2)
            - (2 * ACOS(-1) * dt_pct_of_year)
        ) AS dt_viz_pct_year_radians
    FROM with_pct
),

final AS
(
    SELECT
        *,
        COS(dt_viz_pct_year_radians) AS dt_viz_pct_x_pos,
        SIN(dt_viz_pct_year_radians) AS dt_viz_pct_y_pos,
        DENSE_RANK() OVER (ORDER BY dt_year) AS dt_year_id,
        DENSE_RANK() OVER (ORDER BY dt_year_month) AS dt_month_id,
        DENSE_RANK() OVER (
            ORDER BY FORMAT_DATE('%G', dt_y_m_d), FORMAT_DATE('%V', dt_y_m_d)
        ) AS dt_iso_week_id
    FROM with_radians
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dt_y_m_d) AS dt_id,
    dt_y_m_d,
    dt_year,
    dt_month,
    dt_day,
    dt_days_in_year,
    dt_day_of_year,
    dt_days_left_in_year,
    dt_year_start_dt,
    dt_year_end_dt,
    dt_year_month,
    dt_month_start_dt,
    dt_month_end_dt,
    dt_days_in_month,
    dt_month_name,
    dt_month_name_short,
    dt_year_id,
    dt_month_id,
    dt_iso_weekday,
    dt_iso_dow_full,
    dt_iso_dow_short,
    dt_is_weekday,
    dt_is_weekend,
    dt_week_start_dt,
    dt_week_end_dt,
    dt_iso_weeknumber,
    dt_iso_week_id,
    dt_pct_of_year,
    dt_viz_pct_year_radians,
    dt_viz_pct_x_pos,
    dt_viz_pct_y_pos
FROM final
ORDER BY dt_y_m_d;
