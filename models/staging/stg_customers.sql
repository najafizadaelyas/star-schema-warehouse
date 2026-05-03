with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id::varchar                        as customer_id,
        trim(lower(email))                          as email,
        trim(first_name || ' ' || last_name)        as customer_name,
        first_name::varchar                         as first_name,
        last_name::varchar                          as last_name,
        upper(country_code)::varchar(2)             as country_code,
        coalesce(company_name, 'Unknown')::varchar  as company_name,
        lower(customer_segment)::varchar            as customer_segment,
        lower(acquisition_channel)::varchar         as acquisition_channel,
        created_at::timestamp_ntz                   as created_at,
        updated_at::timestamp_ntz                   as updated_at,
        is_deleted::boolean                         as is_deleted,
        _loaded_at::timestamp_ntz                   as _loaded_at
    from source
    where is_deleted = false
)

select * from renamed
