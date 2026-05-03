-- Mart Usage: DAU, WAU, MAU, feature adoption, and engagement metrics
with daily_events as (
    select
        event_date_key                                              as event_date,
        customer_sk,
        event_type,
        event_category,
        feature_name,
        platform,
        device_type,
        engagement_score,
        is_conversion_event,
        is_churn_signal
    from {{ ref('fct_events') }}
    where event_date_key >= dateadd('year', -2, current_date())
),

-- One row per customer per day (for rolling window user counts)
daily_user_activity as (
    select
        event_date,
        customer_sk,
        sum(engagement_score)                                       as user_engagement_score,
        count(*)                                                    as user_event_count
    from daily_events
    group by event_date, customer_sk
),

daily_agg as (
    select
        event_date,
        count(distinct customer_sk)                                 as dau,
        sum(engagement_score)                                       as daily_engagement_score,
        count(*)                                                    as total_events,
        count(case when event_category = 'ENGAGEMENT' then 1 end)  as engagement_events,
        count(case when is_conversion_event then 1 end)             as conversion_events,
        count(case when is_churn_signal then 1 end)                 as churn_signal_events,
        count(distinct feature_name)                                as distinct_features_used
    from daily_events
    group by event_date
),

-- WAU: count distinct users active in any of the last 7 days
wau as (
    select
        a.event_date,
        count(distinct b.customer_sk)                               as wau_7d
    from daily_agg as a
    inner join daily_user_activity as b
        on b.event_date between dateadd('day', -6, a.event_date) and a.event_date
    group by a.event_date
),

-- MAU: count distinct users active in any of the last 28 days
mau as (
    select
        a.event_date,
        count(distinct b.customer_sk)                               as mau_28d
    from daily_agg as a
    inner join daily_user_activity as b
        on b.event_date between dateadd('day', -27, a.event_date) and a.event_date
    group by a.event_date
)

select
    d.event_date,
    d.dau,
    coalesce(w.wau_7d, d.dau)                                       as wau_7d,
    coalesce(m.mau_28d, d.dau)                                      as mau_28d,
    round(d.dau / nullif(coalesce(w.wau_7d, d.dau), 0) * 100, 2)   as dau_wau_ratio_pct,
    round(d.dau / nullif(coalesce(m.mau_28d, d.dau), 0) * 100, 2)  as dau_mau_ratio_pct,
    d.daily_engagement_score,
    round(d.daily_engagement_score / nullif(d.dau, 0), 2)           as avg_engagement_per_user,
    d.total_events,
    d.engagement_events,
    d.conversion_events,
    d.churn_signal_events,
    d.distinct_features_used,
    avg(d.dau) over (
        order by d.event_date::date rows between 6 preceding and current row
    )                                                               as dau_7d_avg,
    avg(d.total_events) over (
        order by d.event_date::date rows between 6 preceding and current row
    )                                                               as events_7d_avg
from daily_agg as d
left join wau as w on d.event_date = w.event_date
left join mau as m on d.event_date = m.event_date
order by d.event_date desc
