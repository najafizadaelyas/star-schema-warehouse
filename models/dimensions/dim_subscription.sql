-- Dim Subscription: SCD2 from snapshot + health metrics
with snapshot_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['subscription_id', 'dbt_updated_at']) }} as subscription_sk,
        subscription_id,
        customer_id,
        product_id,
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
        dbt_valid_from                  as valid_from,
        dbt_valid_to                    as valid_to,
        case when dbt_valid_to is null
            then true else false
        end                             as is_current
    from {{ ref('snp_subscriptions') }}
),

health_data as (
    select
        subscription_id,
        age_days,
        age_months,
        total_invoices,
        paid_invoices,
        failed_invoices,
        payment_health_pct,
        is_churn_risk,
        health_status
    from {{ ref('bv_subscription_health') }}
)

select
    s.subscription_sk,
    s.subscription_id,
    s.customer_id,
    s.product_id,
    s.status,
    s.billing_period,
    s.plan_code,
    s.mrr_usd,
    s.arr_usd,
    s.trial_start_date,
    s.trial_end_date,
    s.start_date,
    s.end_date,
    s.cancelled_at,
    s.cancellation_reason,
    coalesce(h.age_days, 0)                             as age_days,
    coalesce(h.age_months, 0)                           as age_months,
    coalesce(h.payment_health_pct, 100)                 as payment_health_pct,
    coalesce(h.is_churn_risk, false)                    as is_churn_risk,
    coalesce(h.health_status, 'UNKNOWN')                as health_status,
    s.valid_from,
    s.valid_to,
    s.is_current
from snapshot_data as s
left join health_data as h
    on s.subscription_id = h.subscription_id
    and s.is_current = true
