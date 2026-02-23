WITH source AS (
    SELECT * FROM {{ source('source_retail', 'raw_products') }}
),
renamed AS (
    SELECT
        product_id,
        product_name,
        category,
        CAST(unit_price AS NUMBER(10,2)) AS unit_price 
    FROM source
)
SELECT * FROM renamed