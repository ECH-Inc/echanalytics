WITH raw_visit_note AS(
    SELECT *
    FROM {{ source('alaya_care', 'visit_note') }}
    WHERE _etl_is_deleted = FALSE
),
flatten_cols AS(
    SELECT note_id,
        visit_id,
        visit_note_type,
        note,
        note_created_at,
        note_created_by_first_name,
        note_created_by_last_name,
        employee_id,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_visit_note
)
SELECT *
FROM flatten_cols