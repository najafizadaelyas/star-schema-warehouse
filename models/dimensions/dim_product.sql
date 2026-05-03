-- Dim Product: SCD1 (latest record wins — products rarely change fundamentally)
with products as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }}  as product_sk,
        product_id,
        product_name,
        plan_code,
        product_category,
        billing_model,
        base_price_monthly_usd,
        base_price_annual_usd,
        is_active,
        launched_at,
        deprecated_at,
        created_at,
        updated_at
    from {{ ref('stg_products') }}
),

plan_tiers as (
    select
        plan_code,
        plan_name,
        tier_level,
        monthly_price_usd   as list_price_monthly_usd,
        annual_price_usd    as list_price_annual_usd,
        max_users,
        has_sla,
        support_tier
    from {{ ref('raw_plan_tiers') }}
)

select
    p.product_sk,
    p.product_id,
    p.product_name,
    p.plan_code,
    t.plan_name,
    t.tier_level,
    p.product_category,
    p.billing_model,
    p.base_price_monthly_usd,
    p.base_price_annual_usd,
    t.list_price_monthly_usd,
    t.list_price_annual_usd,
    t.max_users,
    t.has_sla,
    t.support_tier,
    p.is_active,
    p.launched_at,
    p.deprecated_at,
    p.created_at,
    p.updated_at
from products as p
left join plan_tiers as t
    on p.plan_code = t.plan_code
