WITH raw_employee_group AS (
    SELECT *
    FROM {{ source('alaya_care', 'employee_group') }}
),

flatten_cols AS (
    SELECT
        employee_id,
        group_id,
        description,
        active,
        start_date::DATE AS start_date,
        end_date::DATE AS end_date,
        profile:phone_main::VARCHAR AS phone_main,
        profile:company::VARCHAR AS company,
        profile:remarks::VARCHAR AS remarks,
        rate,
        lake_id
    FROM raw_employee_group
)

SELECT *
FROM flatten_cols
