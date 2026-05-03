{{
    config(
        unique_key=['hash_key', 'load_date'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select
        {{ hash_key(['customer_id']) }}                             as hash_key,
        {{ current_load_date() }}                                   as load_date,
        cast(null as timestamp_ntz)                                 as load_end_date,
        {{ dbt_utils.generate_surrogate_key([
            'email', 'customer_name', 'country_code',
            'company_name', 'customer_segment', 'acquisition_channel'
        ]) }}                                                       as hashdiff,
        email,
        customer_name,
        first_name,
        last_name,
        country_code,
        company_name,
        customer_segment,
        acquisition_channel,
        created_at,
        updated_at,
        'CRM'                                                       as record_source
    from {{ ref('stg_customers') }}
)

{% if is_incremental() %}
, new_or_changed as (
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
