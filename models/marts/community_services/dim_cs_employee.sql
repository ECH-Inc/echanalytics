WITH stg_employee AS (
    SELECT *
    FROM {{ ref('stg_ac_employee') }}
),
emp_active_department AS (
    SELECT *
    FROM {{ ref('stg_ac_employee_department') }}
    WHERE active = 1
),
emp_departments AS (
    SELECT employee_id,
        LISTAGG(company, ', ') WITHIN GROUP (ORDER BY company) AS departments
    FROM emp_active_department
    GROUP BY employee_id
),
emp_active_group AS (
    SELECT *
    FROM {{ ref('int_employee_group_dates_standardized') }}
    WHERE CURRENT_DATE() >= start_date
        AND CURRENT_DATE() < end_date
),
emp_groups AS (
    SELECT employee_id,
        LISTAGG(company, ', ') AS employee_group
    FROM emp_active_group
    GROUP BY employee_id
),
emp_active_skill AS (
    SELECT *
    FROM {{ ref('int_employee_skill_details') }}
    WHERE expiry_date IS NULL
        OR expiry_date > CURRENT_DATE()
),
emp_skills AS (
    SELECT employee_id,
        LISTAGG(skill_description, ', ') WITHIN GROUP(ORDER BY skill_description) AS skills,
        LISTAGG(type, ', ') WITHIN GROUP(ORDER BY type) AS skills_long
    FROM emp_active_skill
    GROUP BY employee_id
),
employee_details AS (
    SELECT CASE 
                WHEN stg_employee.supplier_id IS NOT NULL THEN 'Supplier'
                WHEN stg_employee.employee_id IN (2,100,101,102,884,893,1360,1361,103,917,918,1335,872) THEN 'Maintenance Account'
                ELSE 'Employee'
            END AS employee_type,
        CASE stg_employee.status
            WHEN 1 THEN 'Active'
            WHEN 2 THEN 'Terminated'
            WHEN 3 THEN 'Suspended'
            WHEN 4 THEN 'On Hold'
            WHEN 5 THEN 'Pending'
            WHEN 6 THEN 'Applicant'
            WHEN 7 THEN 'Rejected'
            ELSE 'Unknown'
        END AS employee_status,
        stg_employee.alayacare_id,
        stg_employee.external_id,
        stg_employee.import_id,
        stg_employee.supplier_id,
        stg_employee.employee_id,
        stg_employee.salutation,
        stg_employee.first_name,
        stg_employee.last_name,
        stg_employee.preferred_name,
        stg_employee.job_title,
        stg_employee.start_date,
        stg_employee.termination_date,
        stg_employee.language_spoken_at_home_code,
        NULLIF(RTRIM(IFNULL(stg_employee.first_name || ' ', '') || IFNULL(stg_employee.last_name, '')), '') AS full_name,
        stg_employee.site_name,

        emp_groups.employee_group AS groups,

        emp_departments.departments,

        stg_employee.health_care_provider_number_cg_clinic,
        stg_employee.health_care_provider_number_ea_clinic,
        stg_employee.health_care_provider_number_hb_clinic,
        stg_employee.health_care_provider_number_mv_clinic,
        stg_employee.health_care_provider_number_vh_clinic,
        stg_employee.birthday,
        stg_employee.email_preferred,
        stg_employee.email,
        stg_employee.phone_main,
        stg_employee.phone_other,
        stg_employee.address,
        stg_employee.suburb,
        stg_employee.state,
        stg_employee.postcode,
        stg_employee.country,
        stg_employee.billing_address,
        stg_employee.billing_suburb,
        stg_employee.billing_state,
        stg_employee.billing_postcode,
        stg_employee.billing_country,
        stg_employee.restrictions,
        stg_employee.restrictions_start_date,
        stg_employee.designation,
        stg_employee.seniority_rank,
        stg_employee.default_availability,
        stg_employee.employee_minimum_daily_capacity,
        stg_employee.employee_maximum_daily_capacity,
        stg_employee.employee_minimum_weekly_capacity,
        stg_employee.employee_maximum_weekly_capacity,
        stg_employee.payroll_number,
        stg_employee.position_type,
        stg_employee.gender_code,
        stg_employee.marital_status,
        stg_employee.country_of_birth_code,
        stg_employee.scheduling_preference,
        stg_employee.abn,
        stg_employee.company,
        stg_employee.has_company_car,
        stg_employee.supplier_expense_account_code,
        stg_employee.consent_to_record_lgbtiqa_status,
        stg_employee.consent_to_share_information,
        stg_employee.username,
        stg_employee.employee_availability,
        stg_employee.is_vendor,
        stg_employee.language,
        stg_employee.last_visit_at,
        stg_employee.has_skills,
        stg_employee.current_default_rate,
        stg_employee.current_default_rate_start_date,
        stg_employee.employee_current_caseload,

        emp_skills.skills,
        emp_skills.skills_long
    FROM stg_employee
    LEFT JOIN emp_groups
        ON emp_groups.employee_id = stg_employee.employee_id
    LEFT JOIN emp_departments
        ON emp_departments.employee_id = stg_employee.employee_id
    LEFT JOIN emp_skills
        ON emp_skills.employee_id = stg_employee.employee_id
)
SELECT *
FROM employee_details