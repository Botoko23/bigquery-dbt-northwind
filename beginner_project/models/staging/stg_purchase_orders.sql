-- dbt tags: tag_name="stg"
with purchase_orders_source as (

    select 
        id,
        supplier_id,
        created_by,
        submitted_date,
        creation_date,
        expected_date,
        shipping_fee,
        taxes,
        payment_date,
        payment_amount,
        payment_method,
        approved_date
    from {{ source('northwind', 'purchase_orders') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from purchase_orders_source