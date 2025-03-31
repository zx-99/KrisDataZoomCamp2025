{{ config(materialized="table") }}

with
    trips_data as (select * from {{ ref("dim_fhv_trips") }}),

    percentiles as (
        select
            year,
            month,
            pickup_zone,
            dropoff_zone,
            percentile_cont(trip_duration, 0.90) over (
                partition by year, month, pickup_locationid, dropoff_locationid
            ) as percentile_90_trip_duration
        from trips_data
        where trip_duration > 0
    )
select distinct year, month, pickup_zone, dropoff_zone, percentile_90_trip_duration
from percentiles
order by year, month
