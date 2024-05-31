with orders_source as (
    select
        id,
        employee_id,
        customer_id,
        order_date,
        shipped_date,
        ship_name,
        ship_address,
        ship_city,
        ship_state_province,
        ship_zip_postal_code,
        ship_country_region,
        shipping_fee,
        taxes,
        payment_type,
        paid_date,
        tax_rate
    from {{ source('northwind', 'orders') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from orders_source