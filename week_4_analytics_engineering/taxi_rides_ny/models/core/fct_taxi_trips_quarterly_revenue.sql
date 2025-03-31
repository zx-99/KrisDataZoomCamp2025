{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ref("fact_trips")}}
)

    select 
    -- Revenue grouping
    service_type, 
    year_quarter as revenue_yq, 
    year as revenue_year,
    quarter as revenue_quarter,
    -- Revenue calculation 
    sum(total_amount) as revenue_quarter_total_amount,

    from trips_data
    where year in (2019, 2020)
    group by 1,2,3,4