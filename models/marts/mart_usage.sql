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

daily_active_users as (
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

rolling_active_users as (
    select
        event_date,
        dau,
        -- WAU: rolling 7-day unique users
        count(distinct customer_sk) over (
            order by event_date::date
            rows between 6 preceding and current row
        )                                                           as wau_7d,
        -- MAU: rolling 28-day unique users
        count(distinct customer_sk) over (
            order by event_date::date
            rows between 27 preceding and current row
        )                                                           as mau_28d,
        daily_engagement_score,
        total_events,
        engagement_events,
        conversion_events,
        churn_signal_events,
        distinct_features_used
    from daily_active_users
),

feature_adoption as (
    select
        event_date,
        feature_name,
        count(distinct customer_sk)                                 as feature_dau,
        count(*)                                                    as feature_event_count,
        sum(engagement_score)                                       as feature_engagement_score
    from daily_events
    where feature_name is not null
    group by event_date, feature_name
)

select
    r.event_date,
    r.dau,
    r.wau_7d,
    r.mau_28d,
    -- Stickiness ratios
    round(r.dau / nullif(r.wau_7d, 0) * 100, 2)                    as dau_wau_ratio_pct,
    round(r.dau / nullif(r.mau_28d, 0) * 100, 2)                   as dau_mau_ratio_pct,
    r.daily_engagement_score,
    round(r.daily_engagement_score / nullif(r.dau, 0), 2)          as avg_engagement_per_user,
    r.total_events,
    r.engagement_events,
    r.conversion_events,
    r.churn_signal_events,
    r.distinct_features_used,
    -- 7-day rolling averages
    avg(r.dau) over (
        order by r.event_date::date rows between 6 preceding and current row
    )                                                               as dau_7d_avg,
    avg(r.total_events) over (
        order by r.event_date::date rows between 6 preceding and current row
    )                                                               as events_7d_avg
from rolling_active_users as r
order by r.event_date desc
