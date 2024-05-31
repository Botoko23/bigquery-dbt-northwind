with stg_products_source as(
    select
        p.id as product_id,
        p.product_code,
        p.product_name,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        s.company as supplier_company
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_suppliers') }} s
    on p.supplier_id = s.id  
),
unique_products_source as (
    select *, row_number() over(partition by product_id) as row_number
    from stg_products_source
)
select 
    product_id,
    product_code,
    product_name,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    supplier_company,
    current_timestamp() as ingestion_timestamp
from unique_products_source
where row_number = 1