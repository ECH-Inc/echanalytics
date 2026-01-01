WITH raw_guid_relation AS (
    SELECT *
    FROM {{ source('alaya_care', 'guid_relation') }}
),

flatten_cols AS (
    SELECT
        id,
        guid_one,
        guid_two,
        idrelation,
        idmethod,
        rating
    FROM raw_guid_relation
)

SELECT *
FROM flatten_cols
