WITH raw_funding_block AS(
    SELECT *
    FROM {{ source('alaya_care', 'funding_block') }}
),
keep_required_columns AS(
    SELECT funding_block_id,
        funding_parent_id,
        master,
        funding_external_id,
        funding_block_start_date,
        funding_block_revision_date,
        funding_block_expiry,
        funding_block_end_date,
        service_id,
        funding_quantity,
        funding_frequency,
        funding_comments,
        funding_frequency_custom_days,
        visit_count,
        account_id,
        branch_id,
        funding_units,
        funding_block_create_date,
        funding_created_by,
        funding_updated_by,
        properties_funding_block:update_time::datetime AS funding_block_update_time,
        properties_funding_block:create_time::datetime AS funding_block_create_time
    FROM raw_funding_block
)
SELECT *
FROM keep_required_columns