-- Dim Plan: directly from seed reference data
select
    {{ dbt_utils.generate_surrogate_key(['plan_code']) }}   as plan_sk,
    plan_tier_id,
    plan_code,
    plan_name,
    tier_level,
    monthly_price_usd,
    annual_price_usd,
    max_users,
    max_api_calls_per_month,
    has_sla,
    support_tier,
    is_active,
    -- Derived pricing fields
    annual_price_usd / nullif(monthly_price_usd, 0) / 12    as annual_discount_factor,
    (1 - (annual_price_usd / nullif(monthly_price_usd, 0) / 12)) * 100 as annual_discount_pct
from {{ ref('raw_plan_tiers') }}
