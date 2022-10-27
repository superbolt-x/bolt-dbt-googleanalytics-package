{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'googleanalytics_raw', 'ecomm_salesperformance' -%}

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH raw_table AS 
    (SELECT 
        {%- for field in fields %}
        {{ get_googleanalytics_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    where date >= (select max(date) from {{ this }})

    {% endif %}
    ),

    staging AS 
    (SELECT date, profile, source_medium, campaign, transaction_id,
        MAX(sessions_to_transaction) as sessions_to_transaction,
        MAX(days_to_transaction) as days_to_transaction,
        AVG(item_quantity) as item_quantity,
        AVG(transaction_revenue) as transaction_revenue
    FROM raw_table
    GROUP BY date, profile, source_medium, campaign, transaction_id
    )

SELECT *,
    date||'_'||profile||'_'||source_medium||'_'||campaign||'_'||transaction_id as unique_key
FROM staging
