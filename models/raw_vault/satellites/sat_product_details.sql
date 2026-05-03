{{
    config(
        unique_key=['hash_key', 'load_date'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select
        {{ hash_key(['product_id']) }}                                  as hash_key,
        {{ current_load_date() }}                                       as load_date,
        cast(null as timestamp_ntz)                                     as load_end_date,
        {{ dbt_utils.generate_surrogate_key([
            'product_name', 'plan_code', 'product_category',
            'billing_model', 'base_price_monthly_usd', 'is_active'
        ]) }}                                                           as hashdiff,
        product_name,
        plan_code,
        product_category,
        billing_model,
        base_price_monthly_usd,
        base_price_annual_usd,
        is_active,
        launched_at,
        deprecated_at,
        'PRODUCT_CATALOG'                                               as record_source
    from {{ ref('stg_products') }}
),

{% if is_incremental() %}
new_or_changed as (
    select src.*
    from source as src
    left join {{ this }} as tgt
        on src.hash_key = tgt.hash_key
        and tgt.load_end_date is null
    where tgt.hash_key is null
       or src.hashdiff != tgt.hashdiff
)

select * from new_or_changed

{% else %}

select * from source

{% endif %}
