with source as(
    select
        o.id as order_id,
        o.customer_id,
        o.employee_id,
        od.product_id,
        od.quantity,
        od.unit_price,
        od.discount,
        date(o.order_date) as order_date,
        o.shipped_date,
        o.ship_name,
        o.ship_address,
        o.ship_city,
        o.ship_state_province,
        o.ship_zip_postal_code,
        o.ship_country_region,
        o.shipping_fee,
        o.paid_date,
        o.payment_type,
        o.taxes,
        o.tax_rate
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_order_details') }} od
    on o.id = od.order_id
    where od.order_id is not null
),
unique_source as (
    select *, row_number() over(partition by order_id) as row_number
    from source
)
select 
    order_id,
    customer_id,
    employee_id,
    product_id,
    quantity,
    unit_price,
    discount,
    order_date,
    shipped_date,
    ship_name,
    ship_address,
    ship_city,
    ship_state_province,
    ship_zip_postal_code,
    ship_country_region,
    shipping_fee,
    paid_date,
    payment_type,
    taxes,
    tax_rate, 
    current_timestamp() as ingestion_timestamp
from unique_source
where row_number = 1
