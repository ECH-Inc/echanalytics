WITH raw_service_code AS (
    SELECT *
    FROM {{ source('alaya_care', 'service_code') }}
),

keep_required_columns AS (
    SELECT
        service_code_id,
        guid,
        name,
        description,
        department_id,
        duration,
        is_disabled,
        allow_virtual_care,
        rating_methodology_type,
        created_by,
        created_at,
        updated_by,
        updated_at,
        REPLACE(department_name, '"', '') AS department_name
    FROM raw_service_code
)

SELECT *
FROM keep_required_columns
