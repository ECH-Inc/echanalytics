WITH stg_employee_group AS(
    SELECT *
    FROM {{ ref('stg_ac_employee_group') }}
),
default_date AS(
    SELECT employee_id,
        group_id,
        description,
        active,
        IFNULL(start_date, '1970-01-01') AS start_date,
        IFNULL(end_date, '9999-12-31') AS end_date,
        phone_main,
        company,
        remarks,
        rate,
        lake_id
    FROM stg_employee_group
)
SELECT *
FROM default_date