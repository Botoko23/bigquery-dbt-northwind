with source as (
    select
        po.id as id,
        po.customer_id as customer_id,
        e.employee_id as employee_id,
        p.product_id as product_id,
        po.quantity,
        po.unit_cost,
        po.date_received,
        po.posted_to_inventory,
        po,submitted_date,
        po.creation_date,
        po.expected_date,
        po.shipping_fee,
        po.taxes,
        po.payment_date,
        po.payment_amount,
        po.payment_method,
        po.approved_date,
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
        d.year as received_year,
        d.month as received_month,
        d.year_day as received_day,
        
    from {{ ref('fact_purchase_order') }} po
    left join {{ ref('dim_employee') }} e
    on po.employee_id = e.employee_id
    left join {{ ref('dim_product') }} p
    on po.product_id = p.product_id 
    left join {{ ref('dim_date') }} d
    on po.date_received = d.full_date 
)
select *, current_timestamp() as insertion_timestamp
from source
