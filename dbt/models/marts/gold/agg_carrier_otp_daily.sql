{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['flight_date', 'carrier_key'],
        cluster_by=['flight_date'],
        tags=['gold', 'agg', 'critical'],
    )
}}

with f as (
    select * from {{ ref('fct_flight_event') }}
    where {{ incremental_window('flight_date_key', var('bts_late_arrival_days')) }}
)

select
    flight_date_key             as flight_date,
    carrier_key,
    count(*)                    as flights_scheduled,
    count_if(not cancelled_flag) as flights_completed,
    count_if(cancelled_flag)    as flights_cancelled,
    100.0 * count_if(arr_delay_min is not null and arr_delay_min <= 14)
        / nullif(count_if(not cancelled_flag), 0)        as on_time_pct,
    avg(arr_delay_min)::float                            as avg_arr_delay_min,
    sum(coalesce(arr_delay_min, 0))::int                 as total_delay_min,
    current_timestamp()                                   as ingested_at
from f
group by 1, 2
