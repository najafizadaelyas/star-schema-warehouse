-- Business Vault: Subscription-level MRR, ARR, and churn health metrics
with subscriptions as (
    select
        hs.hash_key                                     as subscription_hash_key,
        hs.subscription_bk                              as subscription_id,
        ss.status,
        ss.billing_period,
        ss.plan_code,
        ss.mrr_usd,
        ss.arr_usd,
        ss.start_date,
        ss.end_date,
        ss.cancelled_at,
        ss.cancellation_reason,
        lcs.hub_customer_hash_key                       as customer_hash_key,
        lsp.hub_product_hash_key                        as product_hash_key
    from {{ ref('hub_subscription') }} as hs
    inner join {{ ref('sat_subscription_details') }} as ss
        on hs.hash_key = ss.hash_key
        and ss.load_end_date is null
    left join {{ ref('lnk_customer_subscription') }} as lcs
        on hs.hash_key = lcs.hub_subscription_hash_key
    left join {{ ref('lnk_subscription_product') }} as lsp
        on hs.hash_key = lsp.hub_subscription_hash_key
),

invoice_metrics as (
    select
        subscription_id,
        count(*)                                        as total_invoices,
        count(case when invoice_status = 'PAID' then 1 end)     as paid_invoices,
        count(case when invoice_status = 'FAILED' then 1 end)   as failed_invoices,
        sum(case when invoice_status = 'PAID' then net_amount_usd else 0 end) as total_paid_usd,
        max(case when invoice_status = 'PAID' then invoice_date end) as last_paid_date
    from {{ ref('stg_invoices') }}
    group by subscription_id
),

health_scores as (
    select
        s.subscription_hash_key,
        s.subscription_id,
        s.customer_hash_key,
        s.product_hash_key,
        s.status,
        s.billing_period,
        s.plan_code,
        s.mrr_usd,
        s.arr_usd,
        s.start_date,
        s.end_date,
        s.cancelled_at,
        s.cancellation_reason,
        datediff('day', s.start_date, current_date())                   as age_days,
        datediff('month', s.start_date, current_date())                 as age_months,
        coalesce(im.total_invoices, 0)                                  as total_invoices,
        coalesce(im.paid_invoices, 0)                                   as paid_invoices,
        coalesce(im.failed_invoices, 0)                                 as failed_invoices,
        coalesce(im.total_paid_usd, 0)                                  as total_paid_usd,
        im.last_paid_date,
        -- Churn risk flag: failed payments or past_due status
        case
            when s.status in ('cancelled', 'past_due') then true
            when coalesce(im.failed_invoices, 0) > 0 then true
            else false
        end                                                             as is_churn_risk,
        -- Payment health: % invoices paid
        case
            when coalesce(im.total_invoices, 0) = 0 then null
            else round(im.paid_invoices / im.total_invoices * 100, 2)
        end                                                             as payment_health_pct,
        case
            when s.status = 'active' then 'HEALTHY'
            when s.status = 'trialing' then 'TRIAL'
            when s.status = 'past_due' then 'AT_RISK'
            when s.status = 'paused' then 'PAUSED'
            when s.status = 'cancelled' then 'CHURNED'
            else 'UNKNOWN'
        end                                                             as health_status
    from subscriptions as s
    left join invoice_metrics as im
        on s.subscription_id = im.subscription_id
)

select
    *,
    {{ current_load_date() }} as load_date
from health_scores
