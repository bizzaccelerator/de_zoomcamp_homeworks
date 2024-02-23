{{
    config(
        materialized='table'
    )
}}
with fhv_tripsdata as
(
    select *,
    'fhv' as service_type,
    from {{ ref('stg_fhv_tripdata') }}
),
green_tripdata as (
    select *,  
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select tripid, pickup_locationid, dropoff_locationid, pickup_datetime, dropoff_datetime,service_type, from green_tripdata
    union all 
    select tripid, pickup_locationid, dropoff_locationid, pickup_datetime, dropoff_datetime,service_type, from yellow_tripdata
    union all 
    select tripid, pickup_locationid, dropoff_locationid, pickup_datetime, dropoff_datetime,service_type, from fhv_tripsdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid,
    trips_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    -- trips_unioned.SR_flag,
    trips_unioned.service_type
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid