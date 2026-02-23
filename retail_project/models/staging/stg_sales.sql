WITH raw_data AS (
    SELECT * FROM {{ source('source_retail', 'sales_raw') }}
),

cleaned_data AS (
    SELECT 
        transaction_id,
        store_id,
        product_id,
        COALESCE(customer_id, 'CLIENT_INCONNU') AS customer_id, -- Si le client est manquant, on met une valeur par défaut : AJOUTÉE APRÈS LA REVUE !!!
        CAST(timestamp AS TIMESTAMP) AS transaction_date, -- Typage correct des dates
        payment_method,
        quantity,
        IFF(unit_price > 10000, NULL, unit_price) AS unit_price, -- Nettoyage : Si le prix est aberrant (bug à 99999.99), on le met à NULL
        quantity * IFF(unit_price > 10000, 0, unit_price) AS total_amount_line -- Création d'une nouvelle colonne : Montant total de la ligne
    FROM raw_data
)

SELECT * FROM cleaned_data