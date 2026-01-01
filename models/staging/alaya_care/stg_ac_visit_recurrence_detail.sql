WITH raw_visit_recurrence_detail AS (
    SELECT *
    FROM {{ source('alaya_care', 'visit_recurrence_detail') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        visit_id,
        visit_service_rrule_id,
        in_out_of_recurrence_status,
        visit_recurrence,
        recurrence_frequency_type,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_visit_recurrence_detail
)

SELECT *
FROM flatten_cols
