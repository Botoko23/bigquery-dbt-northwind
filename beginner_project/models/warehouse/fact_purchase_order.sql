with source as(
    select
        po.id as id,
        c.id as customer_id,
        e.id as employee_id,
        pod.product_id,
        pod.quantity,
        pod.unit_cost,
        pod.date_received,
        pod.posted_to_inventory,
        po.supplier_id,
        po.created_by,
        po.submitted_date,
        date(po.creation_date) as creation_date,
        po.expected_date,
        po.shipping_fee,
        po.taxes,
        po.payment_date,
        po.payment_amount,
        po.payment_method,
        po.approved_date
    from {{ ref('stg_purchase_orders') }} po
    left join {{ ref('stg_purchase_order_details') }} pod
    on po.id = pod.purchase_order_id
    left join {{ ref('stg_products') }} p
    on pod.product_id = p.id 
    left join {{ ref('stg_order_details') }} od
    on p.id = od.product_id
    left join {{ ref('stg_orders') }} o
    on od.order_id = o.id 
    left join {{ ref('stg_customer') }} c
    on c.id = o.customer_id
    left join {{ ref('stg_employees') }} e
    on po.created_by = e.id
    where o.customer_id is not null
),
unique_source as (
    select *, row_number() over(partition by id) as row_number
    from source
)
select 
    id,
    customer_id,
    employee_id,
    product_id,
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory,
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
    approved_date,
    current_timestamp() as ingestion_timestamp
from unique_source
where row_number = 1