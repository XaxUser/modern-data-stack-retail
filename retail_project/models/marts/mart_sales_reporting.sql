-- La Configuration 
{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    
    -- Le Filtre Incrémental
    {% if is_incremental() %}
        WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
stores AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

-- Le coeur du Schéma en Étoile : La grande jointure
final_star_schema AS (
    SELECT
        -- Faits (Metrics)
        s.transaction_id,
        s.transaction_date,
        s.quantity,
        s.total_amount_line,
        
        -- Dimension : Client
        COALESCE(c.customer_id, s.customer_id) AS customer_id,
        COALESCE(c.first_name, 'Client') AS first_name,
        COALESCE(c.last_name, 'Anonyme') AS last_name,
        COALESCE(c.segment, 'Non renseigné') AS customer_segment,
        COALESCE(c.country, 'Non renseigné') AS country,
        
        -- Dimension : Produit
        p.product_id,
        p.product_name,
        p.category AS product_category,
        p.unit_price,
        
        -- Dimension : Magasin
        st.store_id,
        st.store_name,
        st.city AS store_city,
        st.store_type
        
    FROM sales s
    -- On utilise des LEFT JOIN pour ne jamais perdre une vente, 
    -- même si le client ou le magasin est introuvable
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN products p  ON s.product_id = p.product_id
    LEFT JOIN stores st   ON s.store_id = st.store_id
)

SELECT * FROM final_star_schema