-- Mart Churn: monthly churn rate, NRR, GRR, and cohort-level retention
with monthly_subscriptions as (
    select
        date_trunc('month', start_date)                             as cohort_month,
        date_trunc('month', coalesce(cancelled_at::date, current_date())) as churn_month,
        subscription_id,
        customer_sk,
        plan_code,
        mrr_usd,
        is_churned,
        is_active,
        is_past_due,
        start_date,
        cancelled_at
    from {{ ref('fct_subscriptions') }}
    where is_current = true
        or is_churned = true
),

monthly_counts as (
    select
        date_trunc('month', calendar.date_day)                      as report_month,
        ms.plan_code,
        count(distinct case
            when ms.start_date <= calendar.date_day
            and (ms.cancelled_at is null or ms.cancelled_at > calendar.date_day)
            then ms.subscription_id
        end)                                                        as active_subs,
        count(distinct case
            when ms.cancelled_at::date between
                date_trunc('month', calendar.date_day)
                and last_day(calendar.date_day, 'month')
            then ms.subscription_id
        end)                                                        as churned_subs,
        count(distinct case
            when ms.start_date between
                date_trunc('month', calendar.date_day)
                and last_day(calendar.date_day, 'month')
            then ms.subscription_id
        end)                                                        as new_subs,
        sum(case
            when ms.start_date <= calendar.date_day
            and (ms.cancelled_at is null or ms.cancelled_at > calendar.date_day)
            then ms.mrr_usd else 0
        end)                                                        as active_mrr_usd
    from {{ ref('dim_date') }} as calendar
    cross join monthly_subscriptions as ms
    where calendar.date_day = last_day(calendar.date_day, 'month')
        and calendar.date_day <= current_date()
        and calendar.date_day >= dateadd('year', -3, current_date())
    group by
        date_trunc('month', calendar.date_day),
        ms.plan_code
)

select
    report_month,
    plan_code,
    active_subs,
    churned_subs,
    new_subs,
    active_mrr_usd,
    -- Churn rate: churned / beginning-of-month active
    case
        when lag(active_subs) over (partition by plan_code order by report_month) > 0
        then round(
            churned_subs / lag(active_subs) over (
                partition by plan_code order by report_month
            ) * 100, 4
        )
        else null
    end                                                             as monthly_churn_rate_pct,
    -- Annualised churn
    case
        when lag(active_subs) over (partition by plan_code order by report_month) > 0
        then round(
            (1 - power(
                1 - churned_subs / nullif(lag(active_subs) over (
                    partition by plan_code order by report_month
                ), 0),
                12
            )) * 100, 4
        )
        else null
    end                                                             as annualised_churn_rate_pct,
    -- MRR churn rate
    case
        when lag(active_mrr_usd) over (partition by plan_code order by report_month) > 0
        then round(
            (lag(active_mrr_usd) over (partition by plan_code order by report_month)
            - active_mrr_usd)
            / lag(active_mrr_usd) over (partition by plan_code order by report_month) * 100, 4
        )
        else null
    end                                                             as mrr_churn_rate_pct
from monthly_counts
order by report_month desc, plan_code
