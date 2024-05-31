with products_source as (
    select 
        id,
        cast(left(supplier_ids,1) as integer) as supplier_id,
        product_code,
        product_name,
        standard_cost,
        list_price,
        reorder_level,
        target_level,
        quantity_per_unit,
        discontinued,
        minimum_reorder_quantity,
        category
    from {{ source('northwind', 'products') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from products_source