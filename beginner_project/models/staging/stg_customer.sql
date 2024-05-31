with customer_source as (

    select
        id, 
        company,
        last_name,
        first_name,
        job_title,
        business_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region 
    from {{ source('northwind', 'customer') }}
)

select 
    *, 
    current_timestamp() as ingestion_timestamp
from customer_source
