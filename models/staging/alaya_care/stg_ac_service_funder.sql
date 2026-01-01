WITH raw_service_funder AS (
    SELECT *
    FROM {{ source('alaya_care', 'service_funder') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        service_id,
        service_guid,
        service_funder_id,
        gfb_id,
        payment_method_name,
        payment_method_type,
        percentage,
        is_primary_funder_break,
        service_bill_code_id,
        service_billing_contact_id,
        _etl_is_deleted,
        _etl_updated_at_utc
    FROM raw_service_funder
)

SELECT *
FROM flatten_cols
