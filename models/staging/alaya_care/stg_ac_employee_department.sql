WITH raw_employee_department AS(
    SELECT *
    FROM {{ source('alaya_care', 'employee_department') }}
),
flatten_cols AS (
    SELECT employee_id,
        profile:company::VARCHAR AS company,
        active,
        department_id,
        description,
        start_date
        end_date,
        lake_id
    FROM raw_employee_department
)
SELECT *
FROM flatten_cols