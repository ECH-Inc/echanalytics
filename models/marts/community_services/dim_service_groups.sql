WITH service_fundings AS(
    SELECT *
    FROM {{ ref('int_service_fundings') }}
),
cs_clients AS(
    SELECT *
    FROM {{ ref('dim_cs_clients') }}
),
prepare_tmp_date AS(
    SELECT DISTINCT ac_client_id,
        service_start_date AS tmp_date,
        funder_program
    FROM service_fundings
    UNION ALL
    SELECT DISTINCT ac_client_id,
        service_end_date AS tmp_date,
        funder_program
    FROM service_fundings
),
derive_next_start AS (
    SELECT
        tmp_date,
        ac_client_id,
        funder_program,
        LEAD(tmp_date) OVER (PARTITION BY ac_client_id,
                                        funder_program
                                ORDER BY tmp_date) AS next_tmp_date
    FROM prepare_tmp_date
),
derive_intervals AS (
    SELECT
        ac_client_id,
        funder_program,
        tmp_date AS interval_start,
        next_tmp_date AS interval_end
    FROM derive_next_start
    WHERE next_tmp_date IS NOT NULL
),
active_services AS (
    SELECT
        derive_intervals.ac_client_id,
        derive_intervals.funder_program,
        derive_intervals.interval_start,
        derive_intervals.interval_end,

        service_fundings.service_id
    FROM derive_intervals
    INNER JOIN service_fundings
        ON service_fundings.ac_client_id = derive_intervals.ac_client_id
            AND service_fundings.funder_program = derive_intervals.funder_program
            AND service_fundings.service_start_date <= derive_intervals.interval_start
            AND service_fundings.service_end_date >= derive_intervals.interval_start
),
service_group AS (
    SELECT
        ac_client_id,
        funder_program,
        interval_start AS group_start_date,
        interval_end AS group_end_date,
        LISTAGG(service_id, ', ') WITHIN GROUP (ORDER BY service_id) AS service_group,
        COUNT(service_id) AS service_count
    FROM active_services
    GROUP BY ac_client_id,
        funder_program,
        interval_start,
        interval_end
),
get_client_ac_details AS(
    SELECT service_group.ac_client_id,

        cs_clients.crm_id,
        cs_clients.mac_id,

        service_group.funder_program,
        service_group.group_start_date,
        service_group.group_end_date,
        service_group.service_group,
        service_group.service_count
    FROM service_group
    LEFT JOIN cs_clients
        ON cs_clients.ac_client_id = service_group.ac_client_id
),
gen_group_sk AS(
    SELECT {{ dbt_utils.generate_surrogate_key(['ac_client_id',
                                                'funder_program',
                                                'group_start_date',
                                                'group_end_date']) }} AS service_group_sk,
        ac_client_id,
        crm_id,
        mac_id,
        funder_program,
        group_start_date,
        group_end_date,
        service_group,
        service_count
    FROM get_client_ac_details
)
SELECT *
FROM gen_group_sk