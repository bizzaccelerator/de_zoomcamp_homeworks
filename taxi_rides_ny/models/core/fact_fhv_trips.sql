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
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripsdata.tripid,
    fhv_tripsdata.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripsdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripsdata.pickup_datetime,
    fhv_tripsdata.dropoff_datetime,
    fhv_tripsdata.SR_flag,
    fhv_tripsdata.service_type
from fhv_tripsdata
inner join dim_zones as pickup_zone
on fhv_tripsdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripsdata.dropoff_locationid = dropoff_zone.locationid