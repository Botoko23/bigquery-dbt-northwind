with source as (
    select
    f.order_id,
    f.customer_id,
    f.employee_id,
    f.product_id,
    f.quantity,
    f.unit_price,
    f.discount,
    f.order_date,
    f.shipped_date,
    f.ship_name,
    f.ship_address,
    f.ship_city,
    f.ship_state_province,
    f.ship_zip_postal_code,
    f.ship_country_region,
    f.shipping_fee,
    f.paid_date,
    f.payment_type,
    f.taxes,
    f.tax_rate, 
    c.company as customer_company,
    c.business_phone as customer_business_phone,
    c.address as customer_address,
    c.city as customer_city,
    c.country_region as customer_country_region,
    e.first_name as employee_first_name,
    e.last_name as employee_last_name,
    e.full_name as employee_full_name,
    e.job_title as employee_job_title,
    e.home_phone as employee_home_phone,
    e.notes as employee_notes,
    p.product_code,
    p.product_name, 
    p.standard_cost as product_standard_cost,
    p.list_price as product_list_price,
    p.reorder_level as product_reorder_level,
    p.target_level as product_target_level,
    p.quantity_per_unit as product_qunatity_per_unit,
    p.discontinued as product_is_discontinued,
    p.minimum_reorder_quantity as product_minimum_reorder_quantity,
    p.category as product_category,
    p.supplier_company,
    d.year as order_year,
    d.month as order_month,
    d.year_day as order_day
    from {{ ref('fact_sales') }} f
    left join {{ ref('dim_customer') }} c
    on f.customer_id = c.customer_id
    left join {{ ref('dim_employee') }} e
    on f.employee_id = e.employee_id
    left join {{ ref('dim_product') }} p
    on f.product_id = p.product_id 
    left join {{ ref('dim_date') }} d
    on f.order_date = d.full_date 
)
select *, current_timestamp() as insertion_timestamp
from source
