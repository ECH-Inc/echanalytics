WITH raw_auth_usages AS(
    SELECT *
    FROM {{ source('alaya_care', 'authorization_usages') }}
),
update_date_cols AS(
    SELECT au_id,
        authorization_id,
        authorization_usage_id,
        authorized_hours,
        utilized_hours,
        over_utilized_hours,
        start_date::DATE AS start_date,
        end_date::DATE AS end_date,
        usage_status,
        authorization_usage_items_id,
        usage_item_id,
        usage_item_type,
        utilized,
        over_utilized,
        visit_id,
        visit_datetime::DATE AS visit_date,
        visit_datetime::TIME AS visit_time,
        client_id,
        frequency_type,
        rule_type
    FROM raw_auth_usages
)
SELECT *
FROM update_date_cols