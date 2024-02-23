{{
    config(
        materialized='table'
    )
}}
with trips_data as (
    select * from {{ ref('fact_fhv_trips') }}
)
select 
-- operations grouping
pickup_zone,
{{ dbt.date_trunc("month", "pickup_datetime")}} as operation_month,

service_type,
-- Additional calculations
count(tripid) as total_monthly_trips,
from trips_data
group by 1,2,3