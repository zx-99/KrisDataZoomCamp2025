{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ref("fact_trips")}}
),

percentiles as (
    select
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS percentile_97_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS percentile_95_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS percentile_90_fare_amount
    from trips_data
    where fare_amount > 0 
    and trip_distance > 0 
    and payment_type_description in ('Cash', 'Credit card') 
    and year in (2019, 2020)
)

select distinct
    service_type,
    year,
    month,
    percentile_97_fare_amount,
    percentile_95_fare_amount,
    percentile_90_fare_amount
from percentiles
ORDER by year, month