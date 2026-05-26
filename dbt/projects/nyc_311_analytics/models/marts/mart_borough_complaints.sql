{{ config(materialized='table') }}

select
    borough,
    complaint_type,
    count(*) as total_requests,
    round(avg(resolution_hours)::numeric, 2) as avg_resolution_hours
from {{ ref('stg_311_requests') }}
group by borough, complaint_type
