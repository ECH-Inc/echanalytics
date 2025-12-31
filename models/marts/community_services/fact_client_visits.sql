WITH stg_visit AS(
    SELECT *
    FROM {{ ref('stg_ac_visit') }}
),
past_visit AS(
    SELECT visit_id,
        guid,
        start_at,
        start_at::DATE AS start_date,
        start_at::TIME AS start_time,
        end_at,
        end_at::DATE AS end_date,
        end_at::TIME AS end_time,
        visit_completed,
        adl_complete,
        cancel_code_id,
        service_id,
        service_code_id,
        visit_approval_status,
        visit_scheduled_duration,
        account_type,
        employee_id,
        ac_client_id,
        service_instructions,
        visit_unit_qty,
        visit_hours_bill,
        break_minutes,
        break_hours,
        facility_id,
        client_visit_facility,
        visit_in_facility,
        visit_recurrence,
        has_note,
        has_adl,
        visit_computed_rate,
        service_rrule_id,
        computed_rate_external_id,
        is_paid,
        visit_missed_class,
        visit_revenue,
        visit_on_hold_reason,
        visit_billcode_id,
        visit_bill,
        visit_bill_duration,
        visit_marked_for_resubmission,
        in_out_of_recurrence_status,
        recurrence_frequency_type,
        visit_computed_rate_units
    FROM stg_visit
    WHERE start_at < current_date()
),
cs_clients AS(
    SELECT *
    FROM {{ ref('dim_cs_clients') }}
),
services AS(
    SELECT *
    FROM {{ ref('dim_services') }}
),
get_service_client_details AS(
    SELECT past_visit.visit_id,
        past_visit.guid,
        past_visit.start_at,
        past_visit.start_date,
        past_visit.start_time,
        past_visit.end_at,
        past_visit.end_date,
        past_visit.end_time,
        past_visit.visit_completed,
        past_visit.adl_complete,
        past_visit.cancel_code_id,
        past_visit.service_id,
        past_visit.service_code_id,
        past_visit.visit_approval_status,
        past_visit.visit_scheduled_duration,
        past_visit.account_type,
        past_visit.employee_id,
        past_visit.ac_client_id,

        cs_clients.crm_id,

        past_visit.service_instructions,
        past_visit.visit_unit_qty,
        past_visit.visit_hours_bill,
        past_visit.break_minutes,
        past_visit.break_hours,
        past_visit.facility_id,
        past_visit.client_visit_facility,
        past_visit.visit_in_facility,
        past_visit.visit_recurrence,
        past_visit.has_note,
        past_visit.has_adl,
        past_visit.visit_computed_rate,
        past_visit.service_rrule_id,
        past_visit.computed_rate_external_id,
        past_visit.is_paid,
        past_visit.visit_missed_class,
        past_visit.visit_revenue,
        past_visit.visit_on_hold_reason,
        past_visit.visit_billcode_id,
        past_visit.visit_bill,
        past_visit.visit_bill_duration,
        past_visit.visit_marked_for_resubmission,
        past_visit.in_out_of_recurrence_status,
        past_visit.recurrence_frequency_type,
        past_visit.visit_computed_rate_units,

        services.department_id,
        services.funder_program
    FROM past_visit
    INNER JOIN cs_clients
        ON cs_clients.ac_client_id = past_visit.ac_client_id
    LEFT JOIN services
        ON services.service_id = past_visit.service_id
)
SELECT *
FROM get_service_client_details