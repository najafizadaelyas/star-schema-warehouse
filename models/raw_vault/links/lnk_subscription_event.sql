{{
    config(
        unique_key='hash_key',
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select
        {{ hash_key(['subscription_id', 'event_id']) }}     as hash_key,
        {{ hash_key(['subscription_id']) }}                  as hub_subscription_hash_key,
        {{ hash_key(['event_id']) }}                         as hub_event_hash_key,
        {{ current_load_date() }}                            as load_date,
        'EVENT_TRACKING'                                     as record_source
    from {{ ref('stg_events') }}
    where subscription_id is not null
),

{% if is_incremental() %}
new_records as (
    select src.*
    from source as src
    left join {{ this }} as tgt
        on src.hash_key = tgt.hash_key
    where tgt.hash_key is null
)

select * from new_records

{% else %}

select * from source

{% endif %}
