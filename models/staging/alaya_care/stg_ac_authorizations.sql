WITH raw_authorizations AS(
    SELECT *
    FROM {{ source('alaya_care', 'authorizations') }}
),
update_date_cols AS(
    SELECT authorizations_id,
        authorization_number,
        start_at::DATE AS start_date,
        end_at::DATE AS end_date,
        methodology,
        authorization_status,
        calculated_priority,
        client_id,
        custom_number_of_days,
        notes,
        first_day_of_week,
        is_code_42,
        case_manager_name,
        case_manager_phone,
        case_manager_email,
        state_type_id,
        state_type_name,
        state_type_is_enabled,
        state_type_updated_at,
        state_type_created_at,
        include_non_visit_items,
        program_id,
        rule_type,
        rule_period,
        rule_monthly,
        rule_weekly,
        rule_daily,
        rule_monday,
        rule_tuesday,
        rule_wednesday,
        rule_thursday,
        rule_friday,
        rule_saturday,
        rule_sunday
    FROM raw_authorizations
)
SELECT *
FROM update_date_cols