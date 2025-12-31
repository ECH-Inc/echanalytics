SELECT *
FROM {{ source('alaya_care', 'employee_history') }}