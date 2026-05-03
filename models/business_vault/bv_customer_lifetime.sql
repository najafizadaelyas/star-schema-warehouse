-- Business Vault: Customer Lifetime Value, cohort, and tenure metrics
with customers as (
    select
        hc.hash_key                                     as customer_hash_key,
        hc.customer_bk                                  as customer_id,
        sc.email,
        sc.customer_name,
        sc.company_name,
        sc.customer_segment,
        sc.acquisition_channel,
        sc.country_code,
        sc.created_at                                   as customer_created_at
    from {{ ref('hub_customer') }} as hc
    inner join {{ ref('sat_customer_details') }} as sc
        on hc.hash_key = sc.hash_key
        and sc.load_end_date is null
),

subscriptions as (
    select
        hs.hash_key                                     as subscription_hash_key,
        ss.status,
        ss.mrr_usd,
        ss.arr_usd,
        ss.start_date,
        ss.end_date,
        ss.cancelled_at,
        lcs.hub_customer_hash_key                       as customer_hash_key
    from {{ ref('hub_subscription') }} as hs
    inner join {{ ref('sat_subscription_details') }} as ss
        on hs.hash_key = ss.hash_key
        and ss.load_end_date is null
    inner join {{ ref('lnk_customer_subscription') }} as lcs
        on hs.hash_key = lcs.hub_subscription_hash_key
),

invoices as (
    select
        si.subscription_id,
        sum(si.net_amount_usd)                          as total_revenue_usd,
        count(distinct si.invoice_id)                   as invoice_count,
        min(si.invoice_date)                            as first_invoice_date,
        max(si.invoice_date)                            as last_invoice_date
    from {{ ref('stg_invoices') }} as si
    where si.invoice_status = 'PAID'
    group by si.subscription_id
),

customer_metrics as (
    select
        c.customer_hash_key,
        c.customer_id,
        c.email,
        c.customer_name,
        c.company_name,
        c.customer_segment,
        c.acquisition_channel,
        c.country_code,
        c.customer_created_at,
        date_trunc('month', c.customer_created_at)                  as cohort_month,
        date_trunc('quarter', c.customer_created_at)                as cohort_quarter,
        date_trunc('year', c.customer_created_at)                   as cohort_year,
        datediff('day', c.customer_created_at, current_timestamp()) as tenure_days,
        datediff('month', c.customer_created_at, current_timestamp()) as tenure_months,
        count(distinct s.subscription_hash_key)                     as subscription_count,
        sum(case when s.status = 'active' then 1 else 0 end)        as active_subscription_count,
        sum(coalesce(s.mrr_usd, 0))                                 as current_mrr_usd,
        sum(coalesce(s.arr_usd, 0))                                 as current_arr_usd,
        sum(coalesce(i.total_revenue_usd, 0))                       as total_lifetime_revenue_usd,
        max(case when s.status = 'active' then 1 else 0 end)        as is_active_customer,
        max(case when s.status = 'cancelled' then 1 else 0 end)     as has_churned,
        min(s.start_date)                                           as first_subscription_date,
        max(coalesce(s.cancelled_at::date, s.end_date))             as last_subscription_end_date
    from customers as c
    left join subscriptions as s
        on c.customer_hash_key = s.customer_hash_key
    left join invoices as i
        on s.subscription_hash_key = {{ hash_key(['i.subscription_id']) }}
    group by
        c.customer_hash_key,
        c.customer_id,
        c.email,
        c.customer_name,
        c.company_name,
        c.customer_segment,
        c.acquisition_channel,
        c.country_code,
        c.customer_created_at
)

select
    customer_hash_key,
    customer_id,
    email,
    customer_name,
    company_name,
    customer_segment,
    acquisition_channel,
    country_code,
    customer_created_at,
    cohort_month,
    cohort_quarter,
    cohort_year,
    tenure_days,
    tenure_months,
    subscription_count,
    active_subscription_count,
    current_mrr_usd,
    current_arr_usd,
    total_lifetime_revenue_usd,
    -- Simple CLV: total revenue / tenure months * 24 (2-year projection)
    case
        when tenure_months > 0
        then (total_lifetime_revenue_usd / tenure_months) * 24
        else 0
    end                                                             as clv_24mo_usd,
    is_active_customer,
    has_churned,
    first_subscription_date,
    last_subscription_end_date,
    {{ current_load_date() }}                                       as load_date
from customer_metrics
