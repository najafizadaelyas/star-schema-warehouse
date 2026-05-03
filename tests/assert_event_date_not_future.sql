-- Custom test: event dates must not be in the future (data quality guard)
-- Returns events with occurred_at > current timestamp
select
    event_id,
    customer_id,
    event_type,
    occurred_at,
    _loaded_at
from {{ ref('stg_events') }}
where occurred_at > current_timestamp()
