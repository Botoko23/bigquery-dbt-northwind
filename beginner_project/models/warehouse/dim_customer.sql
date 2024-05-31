with stg_customer_source as (

    select
        id as customer_id,
        company,
        last_name,
        first_name,
        concat(first_name, ' ', last_name) as full_name,
        job_title,
        business_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region
    from {{ ref('stg_customer') }}
),
unique_customers_source as (
    select *, row_number() over(partition by customer_id) as row_number
    from stg_customer_source
)
select 
    customer_id, 
    company, 
    first_name, 
    last_name, 
    full_name, 
    job_title, 
    business_phone,
    fax_number,
    address,
    city,
    state_province,
    zip_postal_code,
    country_region,
    current_timestamp() as ingestion_timestamp
from unique_customers_source 
where row_number = 1


