WITH raw_facility AS(
    SELECT *
    FROM {{ source('alaya_care', 'facility') }}
),
keep_required_columns AS(
    SELECT facility_id,
        profile_id,
        guid,
        branch_id,
        profile:uid::VARCHAR AS uid,
        profile:import_id::VARCHAR AS import_id,
        profile:company::VARCHAR AS company,
        profile:phone_main::VARCHAR AS phone_main,
        profile:name::VARCHAR AS name,
        profile:address_suite::VARCHAR AS address_suite,
        profile:address::VARCHAR AS address,
        profile:city::VARCHAR AS city,
        profile:state::VARCHAR AS state,
        profile:zip::VARCHAR AS zip,
        profile:country::VARCHAR AS country,
        profile:facility_latitude::VARCHAR AS facility_latitude,
        profile:facility_longitude::VARCHAR AS facility_longitude,
        profile:emergency_response_level::VARCHAR AS emergency_response_level,
        created_at,
        created_by,
        updated_at,
        updated_by
    FROM raw_facility
)
SELECT *
FROM keep_required_columns