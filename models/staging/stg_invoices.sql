with source as (
    select * from {{ source('raw', 'invoices') }}
),

renamed as (
    select
        invoice_id::varchar                 as invoice_id,
        subscription_id::varchar            as subscription_id,
        customer_id::varchar                as customer_id,
        upper(invoice_status)::varchar      as invoice_status,
        upper(payment_method)::varchar      as payment_method,
        upper(currency_code)::varchar(3)    as currency_code,
        amount_usd::number(18, 2)           as amount_usd,
        tax_usd::number(18, 2)              as tax_usd,
        discount_usd::number(18, 2)         as discount_usd,
        net_amount_usd::number(18, 2)       as net_amount_usd,
        invoice_date::date                  as invoice_date,
        due_date::date                      as due_date,
        paid_at::timestamp_ntz              as paid_at,
        created_at::timestamp_ntz           as created_at,
        _loaded_at::timestamp_ntz           as _loaded_at
    from source
)

select * from renamed
