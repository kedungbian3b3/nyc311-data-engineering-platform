{{ config(materialized='table') }}

select
    agency,
    max(agency_name) as agency_name,
    count(*) as total_requests,
    count(*) filter (where status = 'Closed') as closed_requests,
    round(avg(resolution_hours)::numeric, 2) as avg_resolution_hours,
    round(percentile_cont(0.5) within group (order by resolution_hours)::numeric, 2) as median_resolution_hours
from {{ ref('stg_311_requests') }}
where agency is not null
group by agency
order by total_requests desc
