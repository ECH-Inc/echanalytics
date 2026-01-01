WITH employee_termination_note AS (
    SELECT *
    FROM {{ source('alaya_care', 'employee_termination_note') }}
),

flatten_cols AS (
    SELECT
        id,
        guid_to,
        properties:branch_id::INT AS branch_id,
        properties:type::VARCHAR AS note_type,
        properties:status::VARCHAR AS not_status,
        properties:content::VARCHAR AS content,
        properties:is_client_coordinator_note::INT
            AS is_client_coordinator_note,
        rn
    FROM employee_termination_note
)

SELECT *
FROM flatten_cols
