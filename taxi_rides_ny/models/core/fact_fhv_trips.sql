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
    fhv_tripsdata.dropoff_locationid,
    fhv_tripsdata.pickup_datetime,
    fhv_tripsdata.dropoff_datetime,
    fhv_tripsdata.SR_flag
from fhv_tripsdata
inner join dim_zones as pickup_zone
on fhv_tripsdata.pickup_locationid = dim_zones.pickup_zone
inner join dim_zones as dropoff_zone
on fhv_tripsdata.dropoff_locationid = dim_zones.dropoff_locationid