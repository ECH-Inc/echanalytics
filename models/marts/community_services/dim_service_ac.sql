WITH int_service AS(
    SELECT *
    FROM {{ ref('int_service_ac') }}
),
stg_service_code AS (
    SELECT *
    FROM {{ ref('stg_ac_service_code') }}
),
dim_client AS(
    SELECT *
    FROM {{ ref('dim_client_ac') }}
),
client_service AS(
    SELECT int_service.service_id,
        int_service.ac_client_id,

        dim_client.crm_id,
        dim_client.mac_id,
        
        int_service.service_description,
        int_service.service_code_id,
        int_service.service_start_date,
        int_service.service_end_date,
        int_service.service_status,
        int_service.service_status_reason,
        int_service.status_effective_date,
        int_service.status_end_date,
        int_service.funder_name,
        int_service.funder_program,

        stg_service_code.department_id,
        stg_service_code.department_name,
        stg_service_code.duration
    FROM int_service
    LEFT JOIN stg_service_code
        ON stg_service_code.service_code_id = int_service.service_code_id
    LEFT JOIN dim_client
        ON dim_client.ac_client_id = int_service.ac_client_id
)
SELECT *
FROM client_service