{{
    config(
        unique_key='subscription_event_sk',
        on_schema_change='sync_all_columns'
    )
}}

-- Fact Subscriptions: subscription lifecycle events (one row per subscription state change)
with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

customers as (
    select customer_id, customer_sk
    from {{ ref('dim_customer') }}
    where is_current = true
),

products as (
    select product_id, product_sk
    from {{ ref('dim_product') }}
),

plans as (
    select plan_code, plan_sk
    from {{ ref('dim_plan') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.subscription_id', 's.updated_at']) }} as subscription_event_sk,
    s.subscription_id,
    coalesce(c.customer_sk, 'UNKNOWN')              as customer_sk,
    coalesce(p.product_sk, 'UNKNOWN')               as product_sk,
    coalesce(pl.plan_sk, 'UNKNOWN')                 as plan_sk,
    s.customer_id,
    s.product_id,
    s.plan_code,
    s.status,
    s.billing_period,
    s.mrr_usd,
    s.arr_usd,
    s.trial_start_date,
    s.trial_end_date,
    s.start_date,
    s.end_date,
    s.cancelled_at,
    s.cancellation_reason,
    -- Derived flags
    case when s.status = 'trialing' then true else false end     as is_trial,
    case when s.status = 'active' then true else false end       as is_active,
    case when s.status = 'cancelled' then true else false end    as is_churned,
    case when s.status = 'past_due' then true else false end     as is_past_due,
    case when s.trial_start_date is not null then true else false end as is_trial_eligible,
    -- Date keys for joining dim_date
    s.start_date                                                 as start_date_key,
    s.end_date                                                   as end_date_key,
    s.cancelled_at::date                                         as cancelled_date_key,
    s.created_at,
    s.updated_at,
    s._loaded_at
from subscriptions as s
left join customers as c
    on s.customer_id = c.customer_id
left join products as p
    on s.product_id = p.product_id
left join plans as pl
    on s.plan_code = pl.plan_code

{% if is_incremental() %}
where s._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
