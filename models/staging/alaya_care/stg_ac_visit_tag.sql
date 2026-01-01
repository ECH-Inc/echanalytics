WITH raw_visit_tag AS (
    SELECT *
    FROM {{ source('alaya_care', 'visit_tag') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        visit_id,
        tag_list,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_visit_tag
)

SELECT *
FROM flatten_cols
