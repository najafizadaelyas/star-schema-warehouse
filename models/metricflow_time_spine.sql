-- MetricFlow time spine model (required by dbt Semantic Layer)
{{
    config(
        materialized='table',
        meta={'time_spine': true}
    )
}}

with days as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="dateadd(year, 3, current_date())"
        )
    }}
)

select cast(date_day as date) as date_day
from days
