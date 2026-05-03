with source as (
    select * from {{ source('raw', 'events') }}
),

renamed as (
    select
        event_id::varchar                   as event_id,
        customer_id::varchar                as customer_id,
        subscription_id::varchar            as subscription_id,
        upper(event_type)::varchar          as event_type,
        upper(event_category)::varchar      as event_category,
        feature_name::varchar               as feature_name,
        session_id::varchar                 as session_id,
        device_type::varchar                as device_type,
        platform::varchar                   as platform,
        occurred_at::timestamp_ntz          as occurred_at,
        occurred_at::date                   as event_date,
        properties::variant                 as event_properties,
        _loaded_at::timestamp_ntz           as _loaded_at
    from source
)

select * from renamed
