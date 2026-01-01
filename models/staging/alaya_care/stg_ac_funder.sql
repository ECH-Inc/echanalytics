WITH raw_funder AS (
    SELECT *
    FROM {{ source('alaya_care', 'funder') }}
),

flatten_funder AS (
    SELECT
        funder_id,
        profile_id,
        guid,
        code,
        name,
        description,
        funder_category,
        frequency,
        organization_code,
        organization_name,
        invoicing_model,
        line_item_limit,
        holiday_multiplier,
        branch_id,
        agency_contact_id,
        billing_contact_id,
        billing_frequency,
        is_disabled,
        disabled_at,
        disabled_by,
        created_at,
        created_by,
        updated_at,
        updated_by
    FROM raw_funder
)

SELECT *
FROM flatten_funder
