WITH client_tags AS (
    SELECT *
    FROM {{ source('alaya_care', 'client_tags') }}
    WHERE tag_list <> ''
),

flatten_cols AS (
    SELECT
        client_id AS ac_client_id,
        properties:guid::INT AS guid,
        RTRIM(tag_list, ',') AS tag_list
    FROM client_tags
)

SELECT *
FROM flatten_cols
