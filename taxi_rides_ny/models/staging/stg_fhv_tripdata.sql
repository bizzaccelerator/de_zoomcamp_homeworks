{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
    select *,
        row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging','fhv_tripsdata') }}
    where dispatching_base_num is not null
    where date(pickup_datetime) between '2019-01-01' and '2019-12-31'
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_flag,

from tripdata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}