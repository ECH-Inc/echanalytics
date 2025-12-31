WITH dim_date AS(
    SELECT *
    FROM vw_dim_date
    WHERE calendar_date < DATE_TRUNC(MONTH, current_date())
),
funder AS(
    SELECT *
    FROM {{ ref('stg_ac_funder') }}
    WHERE description <> ''
),
program AS(
    SELECT DISTINCT description AS funder_program
    FROM funder
),
service_group AS(
    SELECT *
    FROM {{ ref('dim_service_groups') }}
),
next_start AS(
    SELECT service_group_sk,
        ac_client_id,
        crm_id,
        mac_id,
        funder_program,
        group_start_date,
        group_end_date,
        LEAD(group_start_date) OVER(PARTITION BY ac_client_id
                                    ORDER BY group_start_date) AS next_start_date
    FROM service_group
),
cal_gaps AS(
    SELECT service_group_sk,
        ac_client_id,
        crm_id,
        mac_id,
        funder_program,
        group_start_date,
        DATE_TRUNC(MONTH, group_start_date) AS group_start_month,
        group_end_date,
        DATE_TRUNC(MONTH, group_end_date) AS group_end_month,
        next_start_date,
        DATEDIFF(DAY, group_end_date, next_start_date) AS day_gap
    FROM next_start
),
keep_gap AS(
    SELECT service_group_sk,
        ac_client_id,
        funder_program,
        group_start_date,
        TO_CHAR(group_start_month, 'YYYYMMDD') AS group_start_month,
        group_end_date,
        TO_CHAR(group_end_month, 'YYYYMMDD') AS group_end_month,
        next_start_date,
        day_gap
    FROM cal_gaps
    WHERE day_gap > 1
        OR (day_gap IS NULL AND group_end_date < '9999-12-31')
),
admission AS(
    SELECT group_start_month,
        funder_program,
        COUNT(DISTINCT ac_client_id) AS admission_count
    FROM keep_gap
    GROUP BY group_start_month,
        funder_program
),
discharged AS(
    SELECT group_start_month,
        funder_program,
        COUNT(DISTINCT ac_client_id) AS discharged_count
    FROM keep_gap
    GROUP BY group_start_month,
        funder_program
),
prepared_date AS(
    SELECT dim_date.date_id,
        program.funder_program
    FROM dim_date
    JOIN program
),
admission_discharged AS(
    SELECT prepared_date.date_id,
        prepared_date.funder_program,
        IFNULL(admission.admission_count,0) AS admission_count,
        IFNULL(discharged.discharged_count,0) AS discharged_count
    FROM prepared_date
    LEFT JOIN admission
        ON admission.group_start_month = prepared_date.date_id
            AND admission.funder_program = prepared_date.funder_program
    LEFT JOIN discharged
        ON discharged.group_start_month = prepared_date.date_id
            AND discharged.funder_program = prepared_date.funder_program
)
SELECT *
FROM admission_discharged
