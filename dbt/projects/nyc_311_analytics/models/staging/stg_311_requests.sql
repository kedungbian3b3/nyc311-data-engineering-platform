{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'nyc_311_service_requests') }}
),

cleaned as (
    select
        unique_key,
        created_date,
        closed_date,
        created_date::date as created_day,
        date_trunc('hour', created_date) as created_hour,
        extract(hour from created_date)::int as created_hour_of_day,
        extract(isodow from created_date)::int as created_day_of_week,
        upper(nullif(trim(agency), '')) as agency,
        nullif(trim(agency_name), '') as agency_name,
        coalesce(nullif(trim(complaint_type), ''), 'Unspecified') as complaint_type,
        coalesce(nullif(trim(descriptor), ''), 'Unspecified') as descriptor,
        coalesce(nullif(trim(location_type), ''), 'Unspecified') as location_type,
        nullif(trim(incident_zip), '') as incident_zip,
        initcap(nullif(trim(city), '')) as city,
        coalesce(nullif(trim(borough), ''), 'Unspecified') as borough,
        coalesce(nullif(trim(status), ''), 'Unspecified') as status,
        nullif(trim(resolution_description), '') as resolution_description,
        latitude,
        longitude,
        case
            when closed_date is not null and created_date is not null and closed_date >= created_date
                then extract(epoch from (closed_date - created_date)) / 3600.0
            else null
        end as resolution_hours,
        ingest_batch_id,
        ingested_at
    from source
    where unique_key is not null
)

select *
from cleaned
