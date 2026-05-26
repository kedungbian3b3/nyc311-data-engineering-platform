{{ config(materialized='table') }}

select
    status,
    count(*) as total_requests,
    round(100.0 * count(*) / nullif(sum(count(*)) over (), 0), 2) as percentage
from {{ ref('stg_311_requests') }}
group by status
order by total_requests desc
