with order_details_source as (

    select 
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount
    from {{ source('northwind', 'order_details') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from order_details_source