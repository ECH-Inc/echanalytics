WITH service_fundings AS (
    SELECT *
    FROM {{ ref('int_service_fundings') }}
),

service_code AS (
    SELECT *
    FROM {{ ref('stg_ac_service_code') }}
),

dim_customer AS (
    SELECT *
    FROM {{ ref('dim_customer') }}
),

client_service AS (
    SELECT
        service_fundings.service_id,
        service_fundings.ac_client_id,

        dim_customer.crm_id,
        dim_customer.mac_id,

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

        service_code.department_id,
        service_code.department_name,
        service_code.duration
    FROM service_fundings
    LEFT JOIN service_code
        ON service_fundings.service_code_id = service_code.service_code_id
    LEFT JOIN dim_customer
        ON service_fundings.ac_client_id = dim_customer.ac_client_id
)

SELECT *
FROM client_service
