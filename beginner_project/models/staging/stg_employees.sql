with employees_source as (

    select 
        id, 
        company,
        last_name,
        first_name,
        job_title,
        business_phone,
        home_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        notes 
    from {{ source('northwind', 'employees') }}
)

select 
    *,
    current_timestamp() as ingestion_timestamp
from employees_source