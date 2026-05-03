{% snapshot snp_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'email',
            'customer_name',
            'first_name',
            'last_name',
            'country_code',
            'company_name',
            'customer_segment',
            'acquisition_channel'
        ],
        invalidate_hard_deletes=true
    )
}}

select
    customer_id,
    email,
    customer_name,
    first_name,
    last_name,
    country_code,
    company_name,
    customer_segment,
    acquisition_channel,
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
