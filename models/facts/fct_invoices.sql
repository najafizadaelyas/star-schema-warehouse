{{
    config(
        unique_key='invoice_sk',
        on_schema_change='sync_all_columns'
    )
}}

-- Fact Invoices: billing transactions with MRR/ARR contribution
with invoices as (
    select * from {{ ref('stg_invoices') }}
),

customers as (
    select customer_id, customer_sk
    from {{ ref('dim_customer') }}
    where is_current = true
),

subscriptions as (
    select subscription_id, subscription_sk, plan_code, billing_period, mrr_usd
    from {{ ref('dim_subscription') }}
    where is_current = true
),

plans as (
    select plan_code, plan_sk, tier_level
    from {{ ref('dim_plan') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['i.invoice_id']) }}        as invoice_sk,
    i.invoice_id,
    coalesce(c.customer_sk, 'UNKNOWN')                              as customer_sk,
    coalesce(s.subscription_sk, 'UNKNOWN')                          as subscription_sk,
    coalesce(pl.plan_sk, 'UNKNOWN')                                 as plan_sk,
    i.customer_id,
    i.subscription_id,
    coalesce(s.plan_code, 'UNKNOWN')                                as plan_code,
    coalesce(pl.tier_level, 0)                                      as tier_level,
    i.invoice_status,
    i.payment_method,
    i.currency_code,
    i.amount_usd,
    i.tax_usd,
    i.discount_usd,
    i.net_amount_usd,
    -- MRR contribution (normalize annual to monthly)
    case
        when coalesce(s.billing_period, 'monthly') = 'annual'
        then i.net_amount_usd / 12
        else i.net_amount_usd
    end                                                             as mrr_contribution_usd,
    -- ARR contribution
    case
        when coalesce(s.billing_period, 'monthly') = 'annual'
        then i.net_amount_usd
        else i.net_amount_usd * 12
    end                                                             as arr_contribution_usd,
    -- Payment outcome flags
    case when i.invoice_status = 'PAID' then true else false end    as is_paid,
    case when i.invoice_status = 'FAILED' then true else false end  as is_failed,
    case when i.invoice_status = 'REFUNDED' then true else false end as is_refunded,
    i.invoice_date                                                  as invoice_date_key,
    i.due_date                                                      as due_date_key,
    i.paid_at::date                                                 as paid_date_key,
    i.invoice_date,
    i.due_date,
    i.paid_at,
    i.created_at,
    i._loaded_at
from invoices as i
left join customers as c
    on i.customer_id = c.customer_id
left join subscriptions as s
    on i.subscription_id = s.subscription_id
left join plans as pl
    on s.plan_code = pl.plan_code

{% if is_incremental() %}
where i._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
