{{
    config(
        unique_key='event_sk',
        on_schema_change='sync_all_columns'
    )
}}

-- Fact Events: product usage and activity events (event-grain)
with events as (
    select * from {{ ref('stg_events') }}
),

event_types as (
    select event_type_code, event_category, event_name
    from {{ ref('raw_event_types') }}
),

customers as (
    select customer_id, customer_sk
    from {{ ref('dim_customer') }}
    where is_current = true
),

subscriptions as (
    select subscription_id, subscription_sk
    from {{ ref('dim_subscription') }}
    where is_current = true
),

dates as (
    select date_day, date_key
    from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['e.event_id']) }}          as event_sk,
    e.event_id,
    coalesce(c.customer_sk, 'UNKNOWN')                              as customer_sk,
    coalesce(s.subscription_sk, 'UNKNOWN')                          as subscription_sk,
    e.customer_id,
    e.subscription_id,
    e.event_type,
    coalesce(et.event_category, e.event_category)                   as event_category,
    coalesce(et.event_name, e.event_type)                           as event_name,
    e.feature_name,
    e.session_id,
    e.device_type,
    e.platform,
    e.occurred_at,
    e.event_date                                                    as event_date_key,
    -- Engagement scoring
    case
        when e.event_type in ('FEATURE_USED', 'API_CALL', 'EXPORT') then 3
        when e.event_type in ('PAGE_VIEW', 'LOGIN') then 1
        when e.event_type in ('PLAN_UPGRADED', 'TRIAL_CONVERTED') then 5
        when e.event_type in ('INVITE_SENT', 'INVITE_ACCEPTED') then 4
        else 1
    end                                                             as engagement_score,
    -- Conversion flag
    case when e.event_type in (
        'PLAN_UPGRADED', 'TRIAL_CONVERTED', 'SUBSCRIPTION_CREATED'
    ) then true else false end                                      as is_conversion_event,
    -- Churn signal flag
    case when e.event_type in (
        'SUBSCRIPTION_CANCELLED', 'CHURN_RISK_FLAGGED'
    ) then true else false end                                      as is_churn_signal,
    e._loaded_at
from events as e
left join event_types as et
    on e.event_type = et.event_type_code
left join customers as c
    on e.customer_id = c.customer_id
left join subscriptions as s
    on e.subscription_id = s.subscription_id

{% if is_incremental() %}
where e._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
