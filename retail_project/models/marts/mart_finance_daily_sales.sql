{{
    config(
        materialized='incremental',
        unique_key='sales_date',
        incremental_strategy='delete+insert' 
    )
}}

WITH staging_sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}
        WHERE CAST(transaction_date AS DATE) >= (SELECT MAX(sales_date) FROM {{ this }})
    {% endif %}
),

daily_aggregation AS (
    SELECT 
        -- On extrait juste la date (sans l'heure)
        DATE(transaction_date) AS sales_date,
        store_id,
        payment_method,
        
        -- Les KPI (Key Performance Indicators) financiers
        COUNT(transaction_id) AS total_transactions,
        SUM(quantity) AS total_items_sold,
        SUM(total_amount_line) AS total_revenue
        
    FROM staging_sales
    
    -- On exclut les lignes où le prix était un bug (qui ont été mises à NULL dans le staging)
    WHERE total_amount_line IS NOT NULL
    
    -- On groupe par jour, magasin et moyen de paiement
    GROUP BY 
        DATE(transaction_date),
        store_id,
        payment_method
)

SELECT * FROM daily_aggregation
ORDER BY sales_date DESC, total_revenue DESC