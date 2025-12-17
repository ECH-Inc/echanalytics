WITH stg_service AS(
    SELECT *
    FROM {{ ref('stg_ac_service') }}
),
stg_service_code AS (
    SELECT *
    FROM {{ ref('stg_ac_service_code') }}
),
stg_funder AS(
    SELECT *
    FROM {{ ref('stg_ac_funder') }}
),
stg_service_funder AS (
    SELECT *
    FROM {{ ref('stg_ac_service_funder') }}
),
fix_service_date AS(
    SELECT service_id,
        ac_client_id,
        service_description,
        IFNULL(service_start_date, '1970-01-01') AS service_start_date,
        CASE WHEN service_status = 'on_hold' THEN IFNULL(service_projected_end_date, '9999-12-31')
            ELSE COALESCE(service_status_date::DATE, service_projected_end_date, '9999-12-31') END AS service_end_date,
        service_projected_end_date,
        service_status,
        service_status_reason,
        service_status_date::DATE AS status_effective_date,
        service_status_end_date AS status_end_date,

        stg_service_code.department_id,
        stg_service_code.department_name,
        stg_service_code.duration
    FROM stg_service
    LEFT JOIN stg_service_code
        ON stg_service.service_code_id = stg_service_code.service_code_id
),
client_service AS(
    SELECT stg_service_funder.service_id,
        stg_service_funder.service_funder_id,
    
        fix_service_date.ac_client_id,
        fix_service_date.service_description,
        fix_service_date.service_start_date,
        fix_service_date.service_end_date,
        fix_service_date.service_projected_end_date,
        fix_service_date.service_status,
        fix_service_date.service_status_reason,
        fix_service_date.status_effective_date,
        fix_service_date.status_end_date,
        fix_service_date.department_id,
        fix_service_date.department_name,
        fix_service_date.duration,
    
        stg_funder.name AS funder_name,
        stg_funder.description AS funder_program
    FROM stg_service_funder
    LEFT JOIN fix_service_date
        ON stg_service_funder.service_id = fix_service_date.service_id
    LEFT JOIN stg_funder
        ON stg_service_funder.service_funder_id = stg_funder.funder_id
    WHERE fix_service_date.ac_client_id IS NOT NULL
)
SELECT *
FROM client_service