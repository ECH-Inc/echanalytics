WITH service_fundings AS(
    SELECT *
    FROM {{ ref('int_service_fundings') }}
),
stg_service_code AS (
    SELECT *
    FROM {{ ref('stg_ac_service_code') }}
),
dim_clients AS(
    SELECT *
    FROM {{ ref('dim_cs_clients') }}
),
client_service AS(
    SELECT service_fundings.service_id,
        service_fundings.ac_client_id,

        dim_clients.crm_id,
        dim_clients.mac_id,
        
        service_fundings.service_description,
        service_fundings.service_code_id,
        service_fundings.service_start_date,
        service_fundings.service_end_date,
        service_fundings.service_status,
        service_fundings.service_status_reason,
        service_fundings.status_effective_date,
        service_fundings.status_end_date,
        service_fundings.funder_name,
        service_fundings.funder_program,

        stg_service_code.department_id,
        stg_service_code.department_name,
        stg_service_code.duration
    FROM service_fundings
    LEFT JOIN stg_service_code
        ON stg_service_code.service_code_id = service_fundings.service_code_id
    LEFT JOIN dim_clients
        ON dim_clients.ac_client_id = service_fundings.ac_client_id
)
SELECT *
FROM client_service