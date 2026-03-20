{{ config(materialized='table') }}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),
yellow_tripdata as (
    select *,
        cast(null as integer) as trip_type,
        cast(null as numeric) as ehail_fee,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    trips_unioned.*,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from trips_unioned
inner join dim_zones as pickup_zone
    on trips_unioned.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on trips_unioned.dropoff_location_id = dropoff_zone.locationid
qualify row_number() over (partition by trips_unioned.trip_id order by trips_unioned.pickup_datetime) = 1