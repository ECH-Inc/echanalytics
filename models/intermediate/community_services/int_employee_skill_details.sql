WITH stg_emp_skill AS (
    SELECT *
    FROM {{ ref('stg_ac_employee_skill') }}
),

stg_skill AS (
    SELECT *
    FROM {{ ref('stg_ac_skill') }}
),

emp_skill_details AS (
    SELECT
        stg_emp_skill.employee_id,
        stg_emp_skill.client_id,
        stg_emp_skill.skill_id,
        stg_emp_skill.client_specific_skill,

        stg_skill.skill_description,
        stg_skill.type,
        stg_skill.type_details,
        stg_skill.preference_level,

        stg_emp_skill.date_label_value,
        stg_emp_skill.status,
        stg_emp_skill.training_date,
        stg_emp_skill.expiry_date,
        stg_emp_skill.comments,
        stg_emp_skill.guid
    FROM stg_emp_skill
    LEFT JOIN stg_skill
        ON stg_emp_skill.skill_id = stg_skill.skill_id
)

SELECT *
FROM emp_skill_details
