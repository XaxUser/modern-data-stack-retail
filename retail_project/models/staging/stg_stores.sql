WITH source AS (
    SELECT * FROM {{ source('source_retail', 'raw_stores') }}
),
renamed AS (
    SELECT
        store_id,
        store_name,
        city,
        store_type
    FROM source
)
SELECT * FROM renamed