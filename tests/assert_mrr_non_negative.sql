-- Custom test: MRR values must never be negative in fct_invoices
-- Returns rows that violate the assertion (test fails if any rows returned)
select
    invoice_id,
    customer_sk,
    subscription_sk,
    mrr_contribution_usd,
    invoice_date
from {{ ref('fct_invoices') }}
where mrr_contribution_usd < 0
  and is_refunded = false
