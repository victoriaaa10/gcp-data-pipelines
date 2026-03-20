{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(sr_flag as integer) as sr_flag  -- 1=Shared ride, NULL=Non-shared

from {{ source('staging', 'fhv_tripdata') }}
where dispatching_base_num is not null 
    and extract(year from cast(pickup_datetime as timestamp)) = 2019 -- filter for 2019 only
qualify row_number() over (partition by trip_id order by pickup_datetime) = 1  -- remove duplicates based on trip_id, keeping the first record based on pickup_datetime