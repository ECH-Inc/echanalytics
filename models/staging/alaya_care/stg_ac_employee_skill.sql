WITH raw_employee_skill AS (
    SELECT *
    FROM {{ source('alaya_care', 'employee_skill') }}
),

flatten_cols AS (
    SELECT
        employee_id,
        client_id,
        client_specific_skill,
        skill_id,
        skilllabel_id,
        label_1_value,
        label_2_value,
        label_3_value,
        label_4_value,
        date_label_value::DATE AS date_label_value,
        status,
        training_date,
        expiry_date::DATE AS expiry_date,
        comments,
        guid,
        created_at,
        updated_at
    FROM raw_employee_skill
)

SELECT *
FROM flatten_cols
