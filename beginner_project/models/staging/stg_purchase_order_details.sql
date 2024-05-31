with purchase_order_details_source as (

    select 
    id,
    purchase_order_id,
    product_id,
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory
    from {{ source('northwind', 'purchase_order_details') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from purchase_order_details_source