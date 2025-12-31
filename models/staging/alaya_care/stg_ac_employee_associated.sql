WITH raw_employee_associated AS(
    SELECT *
    FROM {{ source('alaya_care', 'employee_associated') }}
),
flatten_cols AS(
    SELECT id,
        client_id,
        employee_id,
        association_status,
        association_start_date,
        association_end_date,
        continuity,
        description
    FROM raw_employee_associated
)
SELECT *
FROM flatten_cols