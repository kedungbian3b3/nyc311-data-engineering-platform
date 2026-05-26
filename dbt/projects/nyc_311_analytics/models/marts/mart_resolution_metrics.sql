{{ config(materialized='table') }}

select
    borough,
    count(*) filter (where resolution_hours is not null) as resolved_records,
    round(avg(resolution_hours)::numeric, 2) as avg_resolution_hours,
    round(percentile_cont(0.5) within group (order by resolution_hours)::numeric, 2) as median_resolution_hours,
    round(max(resolution_hours)::numeric, 2) as max_resolution_hours
from {{ ref('stg_311_requests') }}
where resolution_hours is not null
group by borough
order by avg_resolution_hours desc
