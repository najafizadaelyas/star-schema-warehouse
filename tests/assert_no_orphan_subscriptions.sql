-- Custom test: every subscription in fct_subscriptions must have a valid customer in dim_customer
-- Returns subscription rows with no matching current customer (orphans)
select
    fs.subscription_id,
    fs.customer_id,
    fs.customer_sk
from {{ ref('fct_subscriptions') }} as fs
left join {{ ref('dim_customer') }} as dc
    on fs.customer_sk = dc.customer_sk
where dc.customer_sk is null
  and fs.customer_sk != 'UNKNOWN'
