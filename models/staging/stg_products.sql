with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        product_id::varchar                     as product_id,
        product_name::varchar                   as product_name,
        upper(plan_code)::varchar               as plan_code,
        lower(product_category)::varchar        as product_category,
        lower(billing_model)::varchar           as billing_model,
        base_price_monthly_usd::number(18, 2)   as base_price_monthly_usd,
        base_price_annual_usd::number(18, 2)    as base_price_annual_usd,
        is_active::boolean                      as is_active,
        launched_at::date                       as launched_at,
        deprecated_at::date                     as deprecated_at,
        created_at::timestamp_ntz               as created_at,
        updated_at::timestamp_ntz               as updated_at,
        _loaded_at::timestamp_ntz               as _loaded_at
    from source
)

select * from renamed
