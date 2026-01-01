WITH employee AS (
    SELECT *
    FROM {{ ref('stg_ac_employee') }}
),

emp_active_department AS (
    SELECT *
    FROM {{ ref('stg_ac_employee_department') }}
    WHERE active = 1
),

emp_departments AS (
    SELECT
        employee_id,
        LISTAGG(company, ', ') WITHIN GROUP (ORDER BY company) AS departments
    FROM emp_active_department
    GROUP BY employee_id
),

emp_active_group AS (
    SELECT *
    FROM {{ ref('int_employee_group_dates_standardized') }}
    WHERE
        CURRENT_DATE() >= start_date
        AND CURRENT_DATE() < end_date
),

emp_groups AS (
    SELECT
        employee_id,
        LISTAGG(company, ', ') AS employee_group
    FROM emp_active_group
    GROUP BY employee_id
),

emp_active_skill AS (
    SELECT *
    FROM {{ ref('int_employee_skill_details') }}
    WHERE
        expiry_date IS NULL
        OR expiry_date > CURRENT_DATE()
),

emp_skills AS (
    SELECT
        employee_id,
        LISTAGG(skill_description, ', ') WITHIN GROUP (
            ORDER BY skill_description
        ) AS skills,
        LISTAGG(type, ', ') WITHIN GROUP (ORDER BY type) AS skills_long
    FROM emp_active_skill
    GROUP BY employee_id
),

employee_details AS (
    SELECT
        employee.alayacare_id,
        employee.external_id,
        employee.import_id,
        employee.supplier_id,
        employee.employee_id,
        employee.salutation,
        employee.first_name,
        employee.last_name,
        employee.preferred_name,
        employee.job_title,
        employee.start_date,
        employee.termination_date,
        employee.language_spoken_at_home_code,
        employee.site_name,
        emp_groups.employee_group AS groups,
        emp_departments.departments,
        employee.health_care_provider_number_college_grove,
        employee.health_care_provider_number_encore,
        employee.health_care_provider_number_henley_beach,
        employee.health_care_provider_number_morphett_vale,
        employee.health_care_provider_number_victor_harbor,
        employee.birthday,
        employee.email_preferred,
        employee.email,
        employee.phone_main,
        employee.phone_other,
        employee.address,
        employee.suburb,
        employee.state,
        employee.postcode,
        employee.country,
        employee.billing_address,
        employee.billing_suburb,
        employee.billing_state,
        employee.billing_postcode,
        employee.billing_country,
        employee.restricted_reason,
        employee.restrictions_start_date,
        employee.designation,
        employee.seniority_rank,
        employee.default_availability,
        employee.employee_minimum_daily_capacity,
        employee.employee_maximum_daily_capacity,
        employee.employee_minimum_weekly_capacity,
        employee.employee_maximum_weekly_capacity,
        employee.payroll_number,
        employee.position_type,
        employee.gender_code,
        employee.marital_status,
        employee.country_of_birth_code,
        employee.scheduling_preference,
        employee.abn,
        employee.company,
        employee.has_company_car,
        employee.supplier_expense_account_code,
        employee.consent_to_record_lgbtiqa_status,
        employee.consent_to_share_information,
        employee.username,
        employee.employee_availability,
        employee.is_vendor,
        employee.language,
        employee.last_visit_at,
        employee.has_skills,
        employee.current_default_rate,
        employee.current_default_rate_start_date,
        employee.employee_current_caseload,
        emp_skills.skills,
        emp_skills.skills_long,
        CASE
            WHEN employee.supplier_id IS NOT NULL THEN 'Supplier'
            WHEN
                employee.employee_id IN (
                    2,
                    100,
                    101,
                    102,
                    884,
                    893,
                    1360,
                    1361,
                    103,
                    917,
                    918,
                    1335,
                    872
                )
                THEN 'Maintenance Account'
            ELSE 'Employee'
        END AS employee_type,

        CASE employee.status
            WHEN 1 THEN 'Active'
            WHEN 2 THEN 'Terminated'
            WHEN 3 THEN 'Suspended'
            WHEN 4 THEN 'On Hold'
            WHEN 5 THEN 'Pending'
            WHEN 6 THEN 'Applicant'
            WHEN 7 THEN 'Rejected'
            ELSE 'Unknown'
        END AS employee_status,
        NULLIF(
            RTRIM(
                COALESCE(employee.first_name || ' ', '')
                || COALESCE(employee.last_name, '')
            ),
            ''
        ) AS full_name
    FROM employee
    LEFT JOIN emp_groups
        ON employee.employee_id = emp_groups.employee_id
    LEFT JOIN emp_departments
        ON employee.employee_id = emp_departments.employee_id
    LEFT JOIN emp_skills
        ON employee.employee_id = emp_skills.employee_id
)

SELECT *
FROM employee_details
