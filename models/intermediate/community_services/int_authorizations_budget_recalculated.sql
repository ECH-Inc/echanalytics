WITH stg_ac_authorizations AS(
    SELECT *
    FROM {{ ref('stg_ac_authorizations') }}
),
get_service_cat AS(
    SELECT authorizations_id,
        authorization_number,
        REGEXP_REPLACE(authorization_number, '-.*', '') AS service_category,
        start_date,
        end_date,
        methodology,
        authorization_status,
        client_id,
        rule_type,
        rule_period,
        rule_period/90 AS daily_budget
    FROM stg_ac_authorizations
),
cal_next_start_date AS(
    SELECT authorizations_id,
        authorization_number,
        service_category,
        start_date,
        end_date,
        LEAD(start_date) OVER(PARTITION BY client_id,
                                            service_category
                                ORDER BY start_date) AS next_start_date,
        methodology,
        authorization_status,
        client_id,
        rule_type,
        rule_period,
        daily_budget
    FROM get_service_cat
),
adjust_end_date AS(
    SELECT authorizations_id,
        authorization_number,
        service_category,
        start_date,
        end_date,
        next_start_date,
        CASE WHEN next_start_date IS NULL OR end_date < next_start_date THEN end_date
            ELSE DATEADD(DAY, -1, next_start_date) END AS adjust_end_date,
        methodology,
        authorization_status,
        client_id,
        rule_type,
        rule_period,
        daily_budget
    FROM cal_next_start_date
),
cal_days_in_period AS(
    SELECT authorizations_id,
        authorization_number,
        service_category,
        start_date,
        end_date,
        next_start_date,
        adjust_end_date,
        DATEDIFF(DAY,start_date, adjust_end_date)+1 AS days_in_period,
        methodology,
        authorization_status,
        client_id,
        rule_type,
        rule_period,
        daily_budget
    FROM adjust_end_date
),
adjust_budget AS(
    SELECT authorizations_id,
        authorization_number,
        service_category,
        start_date,
        DATE_TRUNC('Quarter', start_date) AS quarter_start_date,
        LAST_DAY(adjust_end_date, 'quarter') AS quarter_end_date,
        end_date,
        next_start_date,
        adjust_end_date,
        days_in_period,
        methodology,
        authorization_status,
        client_id,
        rule_type,
        rule_period,
        daily_budget,
        CASE WHEN days_in_period < 90 THEN days_in_period * daily_budget
            ELSE rule_period END AS adjust_budget
    FROM cal_days_in_period
),
cal_budget AS(
    SELECT client_id,
        service_category,
        quarter_start_date,
        quarter_end_date,
        methodology,
        LISTAGG(authorizations_id, ', ') AS auth_id_list,
        LISTAGG(authorization_number, ', ') AS auth_number_list,
        SUM(adjust_budget) AS budget
    FROM adjust_budget
    GROUP BY client_id,
        service_category,
        quarter_start_date,
        quarter_end_date,
        methodology
)
SELECT *
FROM adjust_budget