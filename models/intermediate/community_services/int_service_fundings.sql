WITH service AS (
    SELECT *
    FROM {{ ref('stg_ac_service') }}
),

funder AS (
    SELECT *
    FROM {{ ref('stg_ac_funder') }}
),

service_funder AS (
    SELECT *
    FROM {{ ref('stg_ac_service_funder') }}
),

fix_service_date AS (
    SELECT
        service_id,
        ac_client_id,
        service_description,
        service_code_id,
        service_projected_end_date,
        service_status,
        service_status_reason,
        service_status_date::DATE AS status_effective_date,
        service_status_end_date::DATE AS status_end_date,
        COALESCE(service_start_date, '1970-01-01') AS service_start_date,
        CASE
            WHEN service_status IN ('active', 'on_hold') THEN '9999-12-31'
            ELSE service_status_date::DATE
        END AS service_end_date
    FROM service
),

remove_error_services AS (
    SELECT
        service_id,
        ac_client_id,
        service_description,
        service_code_id,
        service_start_date,
        service_end_date,
        service_projected_end_date,
        service_status,
        service_status_reason,
        status_effective_date,
        status_end_date
    FROM fix_service_date
    WHERE service_start_date < service_end_date
),

get_funder_details AS (
    SELECT
        service_funder.service_id,
        service_funder.service_funder_id,

        funder.name AS funder_name,
        funder.description AS funder_program
    FROM service_funder
    LEFT JOIN funder
        ON service_funder.service_funder_id = funder.funder_id
),

service_funder_details AS (
    SELECT
        remove_error_services.service_id,
        remove_error_services.ac_client_id,
        remove_error_services.service_description,
        remove_error_services.service_code_id,
        remove_error_services.service_start_date,
        remove_error_services.service_end_date,
        remove_error_services.service_projected_end_date,
        remove_error_services.service_status,
        remove_error_services.service_status_reason,
        remove_error_services.status_effective_date,
        remove_error_services.status_end_date,

        get_funder_details.service_funder_id,
        get_funder_details.funder_name,
        get_funder_details.funder_program
    FROM remove_error_services
    LEFT JOIN get_funder_details
        ON remove_error_services.service_id = get_funder_details.service_id
)

SELECT *
FROM service_funder_details
