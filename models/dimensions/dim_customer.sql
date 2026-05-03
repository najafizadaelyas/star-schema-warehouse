-- Dim Customer: SCD2 from snapshot + enriched with business vault CLV metrics
with snapshot_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_updated_at']) }} as customer_sk,
        customer_id,
        email,
        customer_name,
        first_name,
        last_name,
        country_code,
        company_name,
        customer_segment,
        acquisition_channel,
        created_at                          as customer_created_at,
        dbt_valid_from                      as valid_from,
        dbt_valid_to                        as valid_to,
        case when dbt_valid_to is null
            then true else false
        end                                 as is_current
    from {{ ref('snp_customers') }}
),

clv_data as (
    select
        customer_id,
        cohort_month,
        cohort_quarter,
        cohort_year,
        tenure_days,
        tenure_months,
        current_mrr_usd,
        current_arr_usd,
        total_lifetime_revenue_usd,
        clv_24mo_usd,
        is_active_customer,
        has_churned,
        first_subscription_date
    from {{ ref('bv_customer_lifetime') }}
)

select
    s.customer_sk,
    s.customer_id,
    s.email,
    s.customer_name,
    s.first_name,
    s.last_name,
    s.country_code,
    s.company_name,
    s.customer_segment,
    s.acquisition_channel,
    s.customer_created_at,
    coalesce(c.cohort_month, date_trunc('month', s.customer_created_at))   as cohort_month,
    coalesce(c.cohort_quarter, date_trunc('quarter', s.customer_created_at)) as cohort_quarter,
    coalesce(c.cohort_year, date_trunc('year', s.customer_created_at))     as cohort_year,
    coalesce(c.tenure_days, 0)                                             as tenure_days,
    coalesce(c.tenure_months, 0)                                           as tenure_months,
    coalesce(c.current_mrr_usd, 0)                                         as current_mrr_usd,
    coalesce(c.current_arr_usd, 0)                                         as current_arr_usd,
    coalesce(c.total_lifetime_revenue_usd, 0)                              as total_lifetime_revenue_usd,
    coalesce(c.clv_24mo_usd, 0)                                            as clv_24mo_usd,
    coalesce(c.is_active_customer, 0)                                      as is_active_customer,
    coalesce(c.has_churned, 0)                                             as has_churned,
    c.first_subscription_date,
    s.valid_from,
    s.valid_to,
    s.is_current
from snapshot_data as s
left join clv_data as c
    on s.customer_id = c.customer_id
    and s.is_current = true
