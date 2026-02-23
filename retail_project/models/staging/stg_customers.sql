WITH source AS (
    SELECT * FROM {{ source('source_retail', 'raw_customers') }}
),
renamed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        country,
        segment
    FROM source
)
SELECT * FROM renamed