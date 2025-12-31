WITH raw_department AS(
    SELECT *
    FROM {{ source('alaya_care', 'department') }}
),
flatten_cols AS (
    SELECT profile_id,
        department_id,
        branch_id,
        guid,
        profile:phone_main::VARCHAR AS phone_main,
        profile:company::VARCHAR AS company,
        profile:remarks::VARCHAR AS remarks
    FROM raw_department
)
SELECT *
FROM flatten_cols