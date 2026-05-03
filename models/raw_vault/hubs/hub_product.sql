{{
    config(
        unique_key='hash_key',
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select
        {{ hash_key(['product_id']) }}  as hash_key,
        product_id                      as product_bk,
        {{ current_load_date() }}       as load_date,
        'PRODUCT_CATALOG'               as record_source
    from {{ ref('stg_products') }}
)

{% if is_incremental() %}
, new_records as (
    select src.*
    from source as src
    left join {{ this }} as tgt on src.hash_key = tgt.hash_key
    where tgt.hash_key is null
)
select * from new_records

{% else %}
select * from source
{% endif %}
