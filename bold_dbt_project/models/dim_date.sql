{{ 
    config(materialized='table')
}}

WITH date_range AS (
    SELECT
        DATEADD ( 'YEAR' , -4 , DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) ) AS from_date
        , DATEADD ( 'DAY' , -1 , DATEADD ( 'YEAR' , 3 , DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) ) ) AS to_date
)

, date_spine AS (
    SELECT
        from_date AS date
    FROM
        date_range
    UNION ALL
    SELECT
        DATEADD ( 'DAY' , 1 , parent.date ) AS date
    FROM
        date_range child
    INNER JOIN date_spine parent ON ( parent.date < child.to_date )
)

SELECT
    ds.date AS date
    , DATE_TRUNC ( 'MONTH' , ds.date ) AS date_month
    , DATE_TRUNC ( 'YEAR' , ds.date ) AS date_year
    , DAY ( ds.date ) AS day_num
    , MONTH ( ds.date ) AS month_num
    , MONTHNAME ( ds.date ) AS month_name
    , YEAR ( ds.date ) AS year_num
    , DAYOFWEEK ( ds.date ) BETWEEN 1 AND 5 AS is_week_day
    , ds.date = CURRENT_DATE() AS is_current_day
    , ds.date = DATEADD ( 'DAY' , -1 , CURRENT_DATE() ) AS is_last_day
    , DATE_TRUNC ( 'MONTH' , ds.date ) = DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) AS is_current_month
    , DATE_TRUNC ( 'MONTH' , ds.date ) = DATEADD ( 'MONTH' , -1 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) AS is_last_month
    , DATE_TRUNC ( 'YEAR' , ds.date ) = DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) AS is_current_year
    , DATE_TRUNC ( 'YEAR' , ds.date ) = DATEADD ( 'YEAR' , -1 , DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) ) AS is_last_year
    , ds.date BETWEEN DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) AND DATEADD ( 'DAY' , -1 , CURRENT_DATE() ) AS is_mtd
    , ds.date BETWEEN DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) AND DATEADD ( 'DAY' , -1 , CURRENT_DATE() ) AS is_ytd
    , ds.date BETWEEN DATEADD ( 'YEAR' , -1 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) AND DATEADD ( 'YEAR' , -1 , DATEADD ( 'DAY' , -1 , CURRENT_DATE() ) ) AS is_mtd_last_year
    , ds.date BETWEEN DATEADD ( 'YEAR' , -1 , DATE_TRUNC ( 'YEAR' , CURRENT_DATE() ) ) AND DATEADD ( 'YEAR' , -1 , DATEADD ( 'DAY' , -1 , CURRENT_DATE() ) ) AS is_ytd_last_year
    , ds.date BETWEEN DATEADD ( 'MONTH' , -12 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) AND DATEADD ( 'DAY' , -1 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) AS is_last_12_months
    , ds.date BETWEEN DATEADD ( 'MONTH' , -24 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) AND DATEADD ( 'DAY' , -1 , DATEADD ( 'MONTH' , -12 , DATE_TRUNC ( 'MONTH' , CURRENT_DATE() ) ) ) AS is_prior_12_months
FROM
    date_spine ds