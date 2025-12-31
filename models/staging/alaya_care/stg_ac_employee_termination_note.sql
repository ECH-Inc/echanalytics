WITH raw_employee_term_note AS(
    SELECT *
    FROM {{ source('alaya_care', 'employee_termination_note') }}
),
flatten_cols AS(
    SELECT id,
        guid_to,
        properties:branch_id::INT AS branch_id,
        properties:type::VARCHAR AS type,
        properties:status::VARCHAR AS status,
        properties:content::VARCHAR AS content,
        properties:is_client_coordinator_note::INT AS is_client_coordinator_note,
        rn
    FROM raw_employee_term_note
)
SELECT *
FROM flatten_cols