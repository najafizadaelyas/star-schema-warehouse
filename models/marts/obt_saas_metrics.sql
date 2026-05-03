-- One Big Table: denormalized SaaS metrics for BI tools (Tableau, Looker, Power BI)
-- Grain: one row per customer per month
with customer_months as (
    select
        dd.first_day_of_month                                       as report_month,
        dc.customer_id,
        dc.customer_sk,
        dc.customer_name,
        dc.email,
        dc.company_name,
        dc.customer_segment,
        dc.acquisition_channel,
        dc.country_code,
        dc.cohort_month,
        dc.cohort_quarter,
        dc.cohort_year,
        dc.first_subscription_date,
        dc.is_active_customer,
        dc.has_churned
    from {{ ref('dim_date') }} as dd
    cross join {{ ref('dim_customer') }} as dc
    where dc.is_current = true
        and dd.date_day = dd.first_day_of_month
        and dd.date_day >= dc.cohort_month
        and dd.date_day <= current_date()
),

monthly_revenue as (
    select
        date_trunc('month', fi.invoice_date)                        as report_month,
        fi.customer_sk,
        sum(case when fi.is_paid then fi.mrr_contribution_usd else 0 end) as mrr_usd,
        sum(case when fi.is_paid then fi.arr_contribution_usd else 0 end) as arr_usd,
        sum(case when fi.is_paid then fi.net_amount_usd else 0 end) as collected_usd,
        count(case when fi.is_paid then 1 end)                      as paid_invoices,
        count(case when fi.is_failed then 1 end)                    as failed_invoices
    from {{ ref('fct_invoices') }} as fi
    group by
        date_trunc('month', fi.invoice_date),
        fi.customer_sk
),

monthly_events as (
    select
        date_trunc('month', fe.event_date_key)                      as report_month,
        fe.customer_sk,
        count(*)                                                    as total_events,
        count(distinct fe.event_date_key)                           as active_days,
        count(distinct fe.feature_name)                             as features_used,
        sum(fe.engagement_score)                                    as total_engagement_score,
        count(case when fe.is_conversion_event then 1 end)          as conversion_events,
        count(case when fe.is_churn_signal then 1 end)              as churn_signal_events
    from {{ ref('fct_events') }} as fe
    group by
        date_trunc('month', fe.event_date_key),
        fe.customer_sk
),

active_subscriptions as (
    select
        date_trunc('month', fs.start_date)                          as report_month,
        fs.customer_sk,
        count(distinct fs.subscription_id)                          as subscription_count,
        max(fs.plan_code)                                           as highest_plan_code,
        sum(fs.mrr_usd)                                             as subscription_mrr_usd,
        max(case when fs.is_churned then 1 else 0 end)              as churned_this_month
    from {{ ref('fct_subscriptions') }} as fs
    group by
        date_trunc('month', fs.start_date),
        fs.customer_sk
)

select
    cm.report_month,
    cm.customer_id,
    cm.customer_name,
    cm.email,
    cm.company_name,
    cm.customer_segment,
    cm.acquisition_channel,
    cm.country_code,
    cm.cohort_month,
    cm.cohort_quarter,
    cm.cohort_year,
    cm.first_subscription_date,
    cm.is_active_customer,
    cm.has_churned,
    -- Revenue
    coalesce(mr.mrr_usd, 0)                                         as mrr_usd,
    coalesce(mr.arr_usd, 0)                                         as arr_usd,
    coalesce(mr.collected_usd, 0)                                   as collected_usd,
    coalesce(mr.paid_invoices, 0)                                   as paid_invoices,
    coalesce(mr.failed_invoices, 0)                                 as failed_invoices,
    -- Usage
    coalesce(me.total_events, 0)                                    as total_events,
    coalesce(me.active_days, 0)                                     as active_days,
    coalesce(me.features_used, 0)                                   as features_used,
    coalesce(me.total_engagement_score, 0)                          as total_engagement_score,
    coalesce(me.conversion_events, 0)                               as conversion_events,
    coalesce(me.churn_signal_events, 0)                             as churn_signal_events,
    -- Subscriptions
    coalesce(asub.subscription_count, 0)                            as subscription_count,
    coalesce(asub.highest_plan_code, 'FREE')                        as plan_code,
    coalesce(asub.subscription_mrr_usd, 0)                          as subscription_mrr_usd,
    coalesce(asub.churned_this_month, 0)                            as churned_this_month,
    -- Cohort month number (months since first subscription)
    datediff('month', cm.cohort_month, cm.report_month)             as cohort_age_months,
    -- Health score (0-100)
    least(100, greatest(0,
        coalesce(me.active_days, 0) * 3
        + coalesce(me.features_used, 0) * 5
        + case when mr.mrr_usd > 0 then 20 else 0 end
        - coalesce(me.churn_signal_events, 0) * 10
    ))                                                              as health_score
from customer_months as cm
left join monthly_revenue as mr
    on cm.report_month = mr.report_month
    and cm.customer_sk = mr.customer_sk
left join monthly_events as me
    on cm.report_month = me.report_month
    and cm.customer_sk = me.customer_sk
left join active_subscriptions as asub
    on cm.report_month = asub.report_month
    and cm.customer_sk = asub.customer_sk
