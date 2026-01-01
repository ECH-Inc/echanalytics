WITH employee_facility AS (
    SELECT *
    FROM {{ source('alaya_care', 'employee_facility') }}
),

flatten_cols AS (
    SELECT
        employee_id,
        facility_id,
        description,
        active,
        start_date,
        end_date,
        profile:emergency_response_level::VARCHAR AS emergency_response_level,
        profile:company::VARCHAR AS company,
        profile:name::VARCHAR AS facility_name,
        profile:phone_main::VARCHAR AS phone_main,
        profile:address_suite::VARCHAR AS address_suite,
        profile:address::VARCHAR AS address,
        profile:city::VARCHAR AS suburb,
        profile:state::VARCHAR AS state,
        profile:zip::VARCHAR AS postcode,
        profile:country::VARCHAR AS country,
        profile:facility_latitude::VARCHAR AS facility_latitude,
        profile:facility_longitude::VARCHAR AS facility_longitude,
        profile:schedule_patient::VARCHAR AS schedule_patient,
        rate,
        lake_id
    FROM employee_facility
)

SELECT *
FROM flatten_cols
