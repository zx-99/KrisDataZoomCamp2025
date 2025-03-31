{{
    config(
        materialized='table'
    )
}}

with quarter_data as (
    select * from {{ref("fct_taxi_trips_quarterly_revenue")}}
)

    select
    service_type,
    revenue_yq,
    revenue_year,
    revenue_quarter,
    revenue_quarter_total_amount as current_year_revenue,
    
    LAG(revenue_quarter_total_amount) OVER (PARTITION BY service_type, revenue_quarter ORDER BY revenue_year) AS last_year_revenue,
    
    CASE
    WHEN LAG(revenue_quarter_total_amount) OVER (PARTITION BY service_type, revenue_quarter ORDER BY revenue_year) IS NOT NULL
    THEN (revenue_quarter_total_amount - LAG(revenue_quarter_total_amount) OVER (PARTITION BY service_type, revenue_quarter ORDER BY revenue_year)) / LAG(revenue_quarter_total_amount) OVER (PARTITION BY service_type, revenue_quarter ORDER BY revenue_year) * 100
    ELSE NULL
    END AS yoy_percentage
    
    from quarter_data