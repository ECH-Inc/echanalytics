WITH raw_group AS(
    SELECT *
    FROM {{ source('alaya_care', 'group') }}
),
flatten_cols AS(
    SELECT group_id,
        guid,
        profile_id,
        branch_id,
        profile:phone_main::VARCHAR AS phone_main,
        profile:company::VARCHAR AS company,
        profile:remarks::VARCHAR AS remarks
    FROM raw_group
)
SELECT *
FROM flatten_cols