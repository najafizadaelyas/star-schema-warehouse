with source as (
    select * from {{ source('raw', 'subscriptions') }}
),

renamed as (
    select
        subscription_id::varchar                as subscription_id,
        customer_id::varchar                    as customer_id,
        product_id::varchar                     as product_id,
        lower(status)::varchar                  as status,
        lower(billing_period)::varchar          as billing_period,
        plan_code::varchar                      as plan_code,
        mrr_usd::number(18, 2)                  as mrr_usd,
        arr_usd::number(18, 2)                  as arr_usd,
        trial_start_date::date                  as trial_start_date,
        trial_end_date::date                    as trial_end_date,
        start_date::date                        as start_date,
        end_date::date                          as end_date,
        cancelled_at::timestamp_ntz             as cancelled_at,
        cancellation_reason::varchar            as cancellation_reason,
        created_at::timestamp_ntz               as created_at,
        updated_at::timestamp_ntz               as updated_at,
        _loaded_at::timestamp_ntz               as _loaded_at
    from source
)

select * from renamed
