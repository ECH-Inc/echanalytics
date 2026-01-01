WITH raw_visit_daily_activity AS (
    SELECT *
    FROM {{ source('alaya_care', 'visit_daily_activity') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        visit_adl_id,
        visit_adl_start_at,
        visit_adl_end_at,
        visit_adl_status,
        visit_adl_completed,
        visit_adl_completed_by,
        visit_adl_completion_context,
        visit_adl_properties:not_completed_reason_name::VARCHAR
            AS not_completed_reason_name,
        visit_adl_duration,
        visit_adl_is_ad_hoc,
        visit_adl_properties:schedule_item_id::VARCHAR AS schedule_item_id,
        visit_adl_work_session_id,
        visit_adl_comment,
        visit_adl_position,
        visit_adl_created_at,
        visit_adl_updated_at,
        daily_activity_id,
        adl_start_date,
        adl_start_time,
        adl_end_date,
        adl_end_time,
        care_plan_id,
        adl_rank,
        adl_name,
        adl_description,
        adl_type,
        adl_count,
        adl_recurrence_id,
        adl_is_from_library,
        adl_guid,
        adl_has_duration,
        adl_properties:duration::VARCHAR AS adl_duration,
        adl_completed_at,
        adl_status,
        adl_department_id,
        adl_is_required,
        adl_include_485,
        adl_interval,
        adl_frequency_type,
        adl_timezone,
        adl_weekdays,
        adl_module_id,
        adl_properties:available_from::VARCHAR AS available_from,
        adl_properties:available_to::VARCHAR AS available_to,
        adl_properties:client_id::VARCHAR AS ac_client_id,
        adl_module_name,
        adl_latest_revision,
        adl_monthday,
        adl_completed_by,
        adl_completion_note,
        adl_created_at,
        create_user_id,
        adl_updated_at,
        adl_updated_by,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_visit_daily_activity
)

SELECT *
FROM flatten_cols
