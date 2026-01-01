WITH raw_skill AS (
    SELECT *
    FROM {{ source('alaya_care', 'skill') }}
),

flatten_cols AS (
    SELECT
        skill_id,
        branch_id,
        skill_description,
        label_1_name
            AS label_2_name,
        label_3_name,
        type,
        type_details,
        acquired_date_label_name,
        is_client_specific,
        preference_level,
        date_label_name,
        properties_tbl_employee_skill:has_date::VARCHAR AS has_date,
        properties_tbl_employee_skill:has_label1::VARCHAR AS has_label_1,
        properties_tbl_employee_skill:has_acquired_date::VARCHAR
            AS has_acquired_date,
        properties_tbl_employee_skill:has_label2::VARCHAR AS has_label_2,
        properties_tbl_employee_skill:acquired_date_label::VARCHAR
            AS acquired_date_label
    FROM raw_skill
)

SELECT *
FROM flatten_cols
