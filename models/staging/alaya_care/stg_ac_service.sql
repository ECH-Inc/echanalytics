WITH raw_service AS (
    SELECT *
    FROM {{ source('alaya_care', 'service') }}
    WHERE
        service_status_reason <> 'Entered in Error'
        AND _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        service_id,
        guid,
        branch_id,
        service_episode_id,
        service_description,
        service_status,
        service_start_date,
        service_revision_date,
        service_notes,
        service_facility,
        service_instructions,
        service_contact_id,
        profile_id,
        service_client_id AS ac_client_id,
        service_code_id,
        service_status_date,
        service_status_end_date,
        service_projected_end_date,
        service_status_reason,
        assigned_employee_id,
        is_disabled,
        reason_code,
        reason_code_date,
        service_add_to_associated_employee,
        service_creation_first_visit_days,
        timezone,
        careplan_type_id,
        service_visit_count,
        funding_methodology,
        branch_timezone,
        service_first_visit_start_date,
        service_last_visit_start_date,
        service_has_activity_code,
        workflow_step,
        is_address_overridden,
        service_contact_id_values,
        created_at,
        created_by,
        updated_at,
        updated_by,
        _etl_updated_at_utc,
        _etl_is_deleted
    FROM raw_service
)

SELECT *
FROM flatten_cols
