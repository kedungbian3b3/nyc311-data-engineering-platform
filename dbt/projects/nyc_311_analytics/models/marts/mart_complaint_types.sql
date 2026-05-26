{{ config(materialized='table') }}

select
    complaint_type,
    count(*) as total_requests,
    count(*) filter (where status = 'Closed') as closed_requests,
    round(avg(resolution_hours)::numeric, 2) as avg_resolution_hours
from {{ ref('stg_311_requests') }}
group by complaint_type
order by total_requests desc
