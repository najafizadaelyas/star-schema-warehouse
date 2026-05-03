{% snapshot snp_subscriptions %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='check',
        check_cols=[
            'status',
            'billing_period',
            'plan_code',
            'mrr_usd',
            'arr_usd',
            'start_date',
            'end_date',
            'cancelled_at',
            'cancellation_reason'
        ],
        invalidate_hard_deletes=true
    )
}}

select
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
    created_at,
    updated_at
from {{ ref('stg_subscriptions') }}

{% endsnapshot %}
