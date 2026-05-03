-- Mart Churn: monthly churn rate, NRR, GRR, and cohort-level retention
with monthly_subscriptions as (
    select
        date_trunc('month', start_date)                                 as cohort_month,
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
),

months as (
    select distinct first_day_of_month as report_month
    from {{ ref('dim_date') }}
    where date_day <= current_date()
      and date_day >= dateadd('year', -3, current_date())
),

monthly_counts as (
    select
        m.report_month,
        ms.plan_code,
        count(distinct case
            when ms.start_date <= m.report_month
            and (ms.cancelled_at is null or ms.cancelled_at > m.report_month)
            then ms.subscription_id
        end)                                                            as active_subs,
        count(distinct case
            when ms.cancelled_at::date >= m.report_month
            and ms.cancelled_at::date < dateadd('month', 1, m.report_month)
            then ms.subscription_id
        end)                                                            as churned_subs,
        count(distinct case
            when ms.start_date >= m.report_month
            and ms.start_date < dateadd('month', 1, m.report_month)
            then ms.subscription_id
        end)                                                            as new_subs,
        sum(case
            when ms.start_date <= m.report_month
            and (ms.cancelled_at is null or ms.cancelled_at > m.report_month)
            then ms.mrr_usd else 0
        end)                                                            as active_mrr_usd
    from months as m
    cross join monthly_subscriptions as ms
    group by m.report_month, ms.plan_code
)

select
    report_month,
    plan_code,
    active_subs,
    churned_subs,
    new_subs,
    active_mrr_usd,
    case
        when lag(active_subs) over (partition by plan_code order by report_month) > 0
        then round(
            churned_subs / lag(active_subs) over (
                partition by plan_code order by report_month
            ) * 100, 4
        )
        else null
    end                                                                 as monthly_churn_rate_pct,
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
    end                                                                 as annualised_churn_rate_pct,
    case
        when lag(active_mrr_usd) over (partition by plan_code order by report_month) > 0
        then round(
            (lag(active_mrr_usd) over (partition by plan_code order by report_month)
            - active_mrr_usd)
            / lag(active_mrr_usd) over (partition by plan_code order by report_month) * 100, 4
        )
        else null
    end                                                                 as mrr_churn_rate_pct
from monthly_counts
order by report_month desc, plan_code
