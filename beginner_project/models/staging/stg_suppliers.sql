-- dbt tags: tag_name="stg"
with source as (

    select 
        id, 
        company,
        last_name,
        first_name,
        job_title,
    from {{ source('northwind', 'suppliers') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source