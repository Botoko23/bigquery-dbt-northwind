with stg_employees_source as (
    select
        id as employee_id,
        company,
        last_name,
        first_name,
        concat(first_name, ' ', last_name) as full_name,
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
    from {{ ref('stg_employees') }}
),
unique_employees_source as (
    select *, row_number() over(partition by employee_id) as row_number
    from stg_employees_source
)
select 
    employee_id,
    company,
    first_name,
    last_name,
    full_name,
    job_title,
    business_phone,
    home_phone,
    fax_number,
    address,
    city,
    state_province,
    zip_postal_code,
    country_region,
    notes,
    current_timestamp() as ingestion_timestamp
from unique_employees_source
where row_number = 1