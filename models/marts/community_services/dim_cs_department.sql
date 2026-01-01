WITH department AS (
    SELECT *
    FROM {{ ref('stg_ac_department') }}
),

selected_cols AS (
    SELECT
        department_id,
        guid,
        company
    FROM department
)

SELECT *
FROM selected_cols
