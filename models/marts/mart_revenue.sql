-- Mart Revenue: MRR/ARR, expansion, contraction, and net revenue metrics by month
with monthly_invoices as (
    select
        date_trunc('month', invoice_date)                           as revenue_month,
        plan_code,
        tier_level,
        customer_sk,
        subscription_sk,
        sum(case when is_paid then mrr_contribution_usd else 0 end) as mrr_usd,
        sum(case when is_paid then arr_contribution_usd else 0 end) as arr_usd,
        sum(case when is_paid then net_amount_usd else 0 end)       as collected_usd,
        sum(case when is_refunded then net_amount_usd else 0 end)   as refunded_usd,
        count(case when is_paid then 1 end)                         as paid_count,
        count(case when is_failed then 1 end)                       as failed_count
    from {{ ref('fct_invoices') }}
    group by
        date_trunc('month', invoice_date),
        plan_code,
        tier_level,
        customer_sk,
        subscription_sk
),

mrr_movements as (
    select
        cur.revenue_month,
        cur.plan_code,
        cur.tier_level,
        cur.customer_sk,
        cur.subscription_sk,
        cur.mrr_usd                                                 as current_mrr,
        coalesce(prev.mrr_usd, 0)                                   as prior_mrr,
        cur.mrr_usd - coalesce(prev.mrr_usd, 0)                    as mrr_change,
        -- Movement categories
        case
            when prev.mrr_usd is null and cur.mrr_usd > 0 then cur.mrr_usd
            else 0
        end                                                         as new_mrr,
        case
            when prev.mrr_usd > 0 and cur.mrr_usd > prev.mrr_usd
            then cur.mrr_usd - prev.mrr_usd
            else 0
        end                                                         as expansion_mrr,
        case
            when prev.mrr_usd > 0 and cur.mrr_usd < prev.mrr_usd and cur.mrr_usd > 0
            then prev.mrr_usd - cur.mrr_usd
            else 0
        end                                                         as contraction_mrr,
        case
            when prev.mrr_usd > 0 and cur.mrr_usd = 0 then prev.mrr_usd
            else 0
        end                                                         as churned_mrr,
        cur.collected_usd,
        cur.refunded_usd,
        cur.paid_count,
        cur.failed_count
    from monthly_invoices as cur
    left join monthly_invoices as prev
        on cur.customer_sk = prev.customer_sk
        and cur.subscription_sk = prev.subscription_sk
        and cur.revenue_month = dateadd('month', 1, prev.revenue_month)
),

aggregated as (
    select
        revenue_month,
        plan_code,
        tier_level,
        sum(current_mrr)                                            as total_mrr_usd,
        sum(current_mrr) * 12                                       as total_arr_usd,
        sum(new_mrr)                                                as new_mrr_usd,
        sum(expansion_mrr)                                          as expansion_mrr_usd,
        sum(contraction_mrr)                                        as contraction_mrr_usd,
        sum(churned_mrr)                                            as churned_mrr_usd,
        sum(new_mrr + expansion_mrr - contraction_mrr - churned_mrr) as net_new_mrr_usd,
        sum(collected_usd)                                          as total_collected_usd,
        sum(refunded_usd)                                           as total_refunded_usd,
        sum(paid_count)                                             as total_paid_invoices,
        sum(failed_count)                                           as total_failed_invoices,
        count(distinct customer_sk)                                 as paying_customers,
        count(distinct subscription_sk)                             as active_subscriptions
    from mrr_movements
    group by revenue_month, plan_code, tier_level
)

select
    *,
    -- NRR: (MRR_start + expansion - contraction - churn) / MRR_start
    case
        when lag(total_mrr_usd) over (
            partition by plan_code order by revenue_month
        ) > 0
        then (
            lag(total_mrr_usd) over (partition by plan_code order by revenue_month)
            + expansion_mrr_usd - contraction_mrr_usd - churned_mrr_usd
        ) / lag(total_mrr_usd) over (partition by plan_code order by revenue_month) * 100
        else null
    end                                                             as nrr_pct,
    -- GRR: (MRR_start - contraction - churn) / MRR_start
    case
        when lag(total_mrr_usd) over (
            partition by plan_code order by revenue_month
        ) > 0
        then (
            lag(total_mrr_usd) over (partition by plan_code order by revenue_month)
            - contraction_mrr_usd - churned_mrr_usd
        ) / lag(total_mrr_usd) over (partition by plan_code order by revenue_month) * 100
        else null
    end                                                             as grr_pct
from aggregated
order by revenue_month desc, tier_level desc
