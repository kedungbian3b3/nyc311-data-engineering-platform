{{ config(materialized='table') }}

select
    created_day,
    count(*) as total_requests,
    count(*) filter (where status = 'Closed') as closed_requests,
    count(*) filter (where status <> 'Closed') as open_or_other_requests,
    round(avg(resolution_hours)::numeric, 2) as avg_resolution_hours
from {{ ref('stg_311_requests') }}
group by created_day
