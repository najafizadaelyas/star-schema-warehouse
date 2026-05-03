-- Cohort Retention Analysis
-- Returns a retention matrix: % of cohort still active N months after acquisition
-- Not a dbt model — run with `dbt compile` + execute against Snowflake directly

with cohort_base as (
    select
        customer_id,
        cohort_month,
        first_subscription_date
    from {{ ref('dim_customer') }}
    where is_current = true
      and first_subscription_date is not null
),

monthly_activity as (
    select
        fi.customer_sk,
        dc.customer_id,
        dc.cohort_month,
        date_trunc('month', fi.invoice_date)                as active_month,
        sum(fi.mrr_contribution_usd)                        as mrr_usd
    from {{ ref('fct_invoices') }} as fi
    inner join {{ ref('dim_customer') }} as dc
        on fi.customer_sk = dc.customer_sk
        and dc.is_current = true
    where fi.is_paid = true
    group by
        fi.customer_sk,
        dc.customer_id,
        dc.cohort_month,
        date_trunc('month', fi.invoice_date)
),

cohort_sizes as (
    select
        cohort_month,
        count(distinct customer_id)                         as cohort_size
    from cohort_base
    group by cohort_month
),

retention_data as (
    select
        ma.cohort_month,
        datediff('month', ma.cohort_month, ma.active_month) as months_since_acquisition,
        count(distinct ma.customer_id)                      as retained_customers,
        sum(ma.mrr_usd)                                     as cohort_mrr_usd
    from monthly_activity as ma
    group by
        ma.cohort_month,
        datediff('month', ma.cohort_month, ma.active_month)
)

select
    rd.cohort_month,
    cs.cohort_size,
    rd.months_since_acquisition,
    rd.retained_customers,
    round(rd.retained_customers / cs.cohort_size * 100, 2)  as retention_rate_pct,
    rd.cohort_mrr_usd,
    round(rd.cohort_mrr_usd / cs.cohort_size, 2)            as mrr_per_cohort_customer
from retention_data as rd
inner join cohort_sizes as cs
    on rd.cohort_month = cs.cohort_month
where rd.months_since_acquisition >= 0
  and rd.months_since_acquisition <= 24
order by
    rd.cohort_month,
    rd.months_since_acquisition
