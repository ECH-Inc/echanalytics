WITH raw_visit_rating_funder AS (
    SELECT *
    FROM {{ source('alaya_care', 'visit_rating_funder') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        pk_id,
        id,
        visit_id,
        funder_id_coalesced,
        funder_id,
        percentage,
        is_primary,
        billcode_id,
        service_id,
        contact_id,
        program_id,
        program_name,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_visit_rating_funder
)

SELECT *
FROM flatten_cols
