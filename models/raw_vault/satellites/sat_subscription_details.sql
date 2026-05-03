{{
    config(
        unique_key=['hash_key', 'load_date'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select
        {{ hash_key(['subscription_id']) }}                             as hash_key,
        {{ current_load_date() }}                                       as load_date,
        cast(null as timestamp_ntz)                                     as load_end_date,
        {{ dbt_utils.generate_surrogate_key([
            'status', 'billing_period', 'plan_code',
            'mrr_usd', 'arr_usd', 'start_date', 'end_date'
        ]) }}                                                           as hashdiff,
        status,
        billing_period,
        plan_code,
        mrr_usd,
        arr_usd,
        trial_start_date,
        trial_end_date,
        start_date,
        end_date,
        cancelled_at,
        cancellation_reason,
        'CRM'                                                           as record_source
    from {{ ref('stg_subscriptions') }}
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
