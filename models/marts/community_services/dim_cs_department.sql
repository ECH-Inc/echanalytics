WITH stg_department AS (
    SELECT *
    FROM {{ ref('stg_ac_department') }}
),
selected_cols AS(
    SELECT department_id,
        guid,
        company
    FROM stg_department
)
SELECT *
FROM selected_cols